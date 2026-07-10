import asyncio
import logging
import queue
from typing import Any

import asyncpg

from dynamic_des.connectors.egress.base import BaseEgress

logger = logging.getLogger(__name__)


class PostgresEgress(BaseEgress):
    """
    Asynchronous egress provider for persisting simulation data to PostgreSQL.

    This connector handles long-term storage of simulation results or 
    continuous generation of relational CDC data, performing bulk inserts 
    of batched data into a specified PostgreSQL table using asyncpg.
    """

    def __init__(
        self, connection_dsn: str, table_name: str = "simulation_data", **kwargs: Any
    ):
        """
        Initializes the PostgresEgress with connection details.

        Args:
            connection_dsn: PostgreSQL connection string (DSN).
            table_name: Target table for simulation records.
            **kwargs: Additional connection pool arguments for asyncpg.
        """
        self.dsn = connection_dsn
        self.table_name = table_name
        self.kwargs = kwargs
        self.pool = None
        self.valid_columns = set()

    async def _init_pool(self) -> None:
        if not self.pool:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.kwargs)
            logger.info(f"PostgresEgress connected to {self.table_name}")
            
            # Cache valid columns to safely ignore extra injected fields (like sim_ts)
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                    self.table_name
                )
                self.valid_columns = {row['column_name'] for row in rows}
                if not self.valid_columns:
                    logger.warning(f"Table '{self.table_name}' does not exist or has no columns!")

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Main execution loop for PostgreSQL data persistence.

        Args:
            egress_queue: A thread-safe queue containing batches of simulation data.
        """
        await self._init_pool()

        while True:
            try:
                # Receive a batch (list) of dictionaries
                batch = egress_queue.get_nowait()
                if not batch:
                    continue

                # Support multi-table multiplexing using a __table__ key
                clean_batch = []
                for item in batch:
                    # `item` is an EventPayload or TelemetryPayload dict.
                    # The actual user data is in `item["value"]`.
                    payload = item.get("value")
                    
                    if not isinstance(payload, dict):
                        # PostgresEgress requires dictionaries (or Pydantic models dumped to dicts)
                        continue
                        
                    # Merge the simulation metadata into the payload so it CAN be inserted if the user configured their schema to accept it
                    payload_copy = payload.copy()
                    payload_copy["sim_ts"] = item.get("sim_ts")
                    payload_copy["timestamp"] = item.get("timestamp")
                    
                    if "key" in item:
                        payload_copy["event_id"] = item["key"]
                    if "path_id" in item:
                        payload_copy["path_id"] = item["path_id"]

                    # Determine target table, defaulting to self.table_name if not specified
                    target = payload_copy.get("__table__", self.table_name)
                    if target == self.table_name:
                        payload_copy.pop("__table__", None)
                        clean_batch.append(payload_copy)

                if not clean_batch:
                    continue

                # Filter keys to only those that exist in the database table
                keys = [k for k in clean_batch[0].keys() if k in self.valid_columns]
                
                if not keys:
                    logger.warning(f"No matching columns for table {self.table_name}. Skipping.")
                    continue

                columns = ", ".join(keys)
                placeholders = ", ".join(f"${i+1}" for i in range(len(keys)))
                
                # Using ON CONFLICT DO NOTHING to ensure idempotency during simulation restarts
                query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                # Extract the tuples of values matching the ordered keys
                values = [tuple(data.get(k) for k in keys) for data in clean_batch]

                # Execute batch insert
                async with self.pool.acquire() as conn:
                    await conn.executemany(query, values)
                
                logger.debug(f"Inserted {len(values)} records into {self.table_name}")

            except queue.Empty:
                # Yield to the event loop if the queue is empty
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error inserting batch into {self.table_name}: {e}")
                # Back-off on database failures to prevent rapid crash looping
                await asyncio.sleep(1)

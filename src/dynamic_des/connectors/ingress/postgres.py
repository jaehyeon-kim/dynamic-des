import asyncio
import json
import logging
import queue
from typing import Any

import asyncpg

from dynamic_des.connectors.ingress.base import BaseIngress

logger = logging.getLogger(__name__)


class PostgresIngress(BaseIngress):
    """
    Asynchronous ingress provider for polling updates from a PostgreSQL database.

    This connector polls a specified configuration table for rows where 'is_applied'
    is FALSE. It parses the parameter path and value, forwards them to the
    simulation registry, and then updates the row to mark it as applied.
    """

    def __init__(
        self,
        connection_dsn: str,
        table_name: str = "simulation_params",
        poll_interval: float = 2.0,
        **kwargs: Any,
    ):
        """
        Initializes the PostgresIngress.

        Args:
            connection_dsn: PostgreSQL connection string (DSN).
            table_name: Table to poll for parameter updates. Must have columns:
                        id (serial), param_path (text), param_value (text/json), is_applied (boolean).
            poll_interval: Seconds to wait between database polls.
            **kwargs: Additional connection pool arguments for asyncpg.
        """
        self.dsn = connection_dsn
        self.table_name = table_name
        self.poll_interval = poll_interval
        self.kwargs = kwargs
        self.pool: asyncpg.Pool | None = None

    async def _init_pool(self) -> None:
        """Initializes the asyncpg connection pool if not already created."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(self.dsn, **self.kwargs)

            # Ensure the table exists
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id SERIAL PRIMARY KEY,
                        param_path VARCHAR(255) NOT NULL,
                        param_value TEXT NOT NULL,
                        is_applied BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Main execution loop fetching dynamic parameter updates from PostgreSQL.

        Args:
            ingress_queue: A thread-safe queue used to transmit (path, value)
                tuples to the SimulationRegistry.
        """
        await self._init_pool()

        # Phase 1: Load the most recent applied state to resume seamlessly
        try:
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                # Get the latest applied value for each distinct param_path
                rows = await conn.fetch(f"""
                    SELECT DISTINCT ON (param_path) param_path, param_value
                    FROM {self.table_name}
                    WHERE is_applied = TRUE
                    ORDER BY param_path, id DESC
                """)
                for row in rows:
                    path = row["param_path"]
                    raw_val = row["param_value"]
                    try:
                        val = json.loads(raw_val)
                    except json.JSONDecodeError:
                        val = raw_val
                    ingress_queue.put((path, val))
                    logger.info(f"Loaded historical parameter state: {path} = {val}")
        except Exception as e:
            logger.error(f"Error loading historical state from {self.table_name}: {e}")

        # Phase 2: Continuously poll for new unapplied updates
        while True:
            try:
                assert self.pool is not None
                async with self.pool.acquire() as conn:
                    # Fetch all unapplied parameter updates
                    rows = await conn.fetch(f"""
                        SELECT id, param_path, param_value
                        FROM {self.table_name}
                        WHERE is_applied = FALSE
                        ORDER BY id ASC
                    """)

                    if rows:
                        applied_ids = []
                        for row in rows:
                            row_id = row["id"]
                            path = row["param_path"]
                            raw_val = row["param_value"]

                            try:
                                # Attempt to parse JSON to maintain types (e.g. ints, dicts)
                                val = json.loads(raw_val)
                            except json.JSONDecodeError:
                                # Fallback to raw string if it's not valid JSON
                                val = raw_val

                            # Send to the simulation registry queue
                            ingress_queue.put((path, val))
                            applied_ids.append(row_id)
                            logger.info(f"Ingested parameter update: {path} = {val}")

                        # Mark the rows as applied so we don't process them again
                        if applied_ids:
                            await conn.execute(
                                f"""
                                UPDATE {self.table_name}
                                SET is_applied = TRUE
                                WHERE id = ANY($1::int[])
                            """,
                                applied_ids,
                            )

            except Exception as e:
                logger.error(
                    f"Error polling PostgresIngress table {self.table_name}: {e}"
                )

            # Wait before polling again
            await asyncio.sleep(self.poll_interval)

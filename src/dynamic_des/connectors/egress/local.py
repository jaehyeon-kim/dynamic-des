import asyncio
import queue

from dynamic_des.connectors.egress.base import BaseEgress


class ConsoleEgress(BaseEgress):
    """Prints simulation results to the console. Handles batched lists."""

    async def run(self, egress_queue: queue.Queue) -> None:
        while True:
            try:
                # Receive a batch (list) of messages
                batch = egress_queue.get_nowait()

                for data in batch:
                    # Identify the stream type for the prefix
                    stream = data.pop("stream_type", "unknown")
                    prefix = "[TELEMETRY]" if stream == "telemetry" else "[EVENT]"

                    # Print the remaining data (path_id/key, value, timestamp)
                    print(f"{prefix} {data}")

            except queue.Empty:
                # Yield to the event loop
                await asyncio.sleep(0.1)

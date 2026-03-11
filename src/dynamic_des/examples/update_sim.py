import asyncio
import sys

from dynamic_des import KafkaAdminConnector


async def send_single_update(path_id: str, value: int):
    # Initialize the admin connector
    connector = KafkaAdminConnector(bootstrap_servers="localhost:9092")

    # Send the update to the config topic
    await connector.send_config(topic="sim-config", path_id=path_id, value=value)
    print(f"Successfully sent: {path_id} = {value}")


if __name__ == "__main__":
    # Check for CLI arguments: path and value
    if len(sys.argv) < 3:
        print(
            "Usage: uv run python src/dynamic_des/examples/update_sim.py <path_id> <value>"
        )
        print(
            "Example: uv run python src/dynamic_des/examples/update_sim.py Line_A.resources.lathe.current_cap 5"
        )
    else:
        target_path = sys.argv[1]
        target_value = int(sys.argv[2])
        asyncio.run(send_single_update(target_path, target_value))

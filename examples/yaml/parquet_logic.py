"""Python for examples/yaml/parquet.yaml: the router, the destination and the start time.

Importing this module creates nothing. The destination is created by
ensure_destination, which the YAML calls through run.before.
"""

import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            nested_value = data.pop("value")
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


use_s3 = os.getenv("USE_S3", "false").lower() == "true"
# odctl-dev is one of the buckets the odctl `storage` profile creates.
base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
filesystem = None

if use_s3:
    from pyarrow import fs

    filesystem = fs.S3FileSystem(
        access_key=os.getenv("S3_ACCESS_KEY", "user"),
        secret_key=os.getenv("S3_SECRET_KEY", "password"),
        endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
        scheme="http",
    )


def ensure_destination() -> None:
    """Create the target directory or bucket path, once the run is about to start."""
    if use_s3 and filesystem is not None:
        logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
        filesystem.create_dir(base_path)
    else:
        logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
        os.makedirs(base_path, exist_ok=True)


router = create_history_router(base_path)

LOGICAL_START_TIME = datetime.now() - timedelta(days=7)

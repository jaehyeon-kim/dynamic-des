"""Python for examples/yaml/iceberg.yaml: the catalog, the router and the schema."""

import os
from datetime import datetime, timedelta

import pyarrow as pa

NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "simulation")
EVENTS_TABLE = f"{NAMESPACE}.events"

# Pinned rather than inferred. Inference reads the ISO timestamp the environment
# writes as a string, which is not what a consumer of an event table expects.
EVENTS_SCHEMA = pa.schema(
    [
        ("sim_ts", pa.float64()),
        ("timestamp", pa.timestamp("us")),
        ("key", pa.string()),
        ("path_id", pa.string()),
        ("status", pa.string()),
    ]
)
SCHEMAS = {EVENTS_TABLE: EVENTS_SCHEMA}


def create_table_router(events_table: str):
    """
    Router Factory: Generates a router returning `namespace.table`, and reshapes
    each event into the pinned schema.
    """

    def table_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        if data.get("stream_type") != "event":
            return None

        # FLATTEN EVENT INTO THE PINNED COLUMNS
        nested_value = data.pop("value", None)
        if isinstance(nested_value, dict):
            data.update(nested_value)

        # A pinned timestamp column takes a datetime. PyArrow rejects the ISO string
        # the environment writes, so the conversion belongs here.
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])

        return events_table

    return table_router


def build_catalog():
    """Connects to the Iceberg REST catalog from the odctl `catalog` profile."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "odctl",
        **{
            "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
            "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "user"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
            "s3.region": os.getenv("S3_REGION", "us-east-1"),
        },
    )


router = create_table_router(EVENTS_TABLE)

# RestCatalog fetches the catalog configuration when it is constructed, so this line
# contacts the catalog as the YAML file is loaded.
catalog = build_catalog()

LOGICAL_START_TIME = datetime.now() - timedelta(days=1)

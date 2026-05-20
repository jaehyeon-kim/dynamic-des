import logging
from importlib.metadata import PackageNotFoundError, version

# ---------------------------------------------------------
# Core & Local Components (Always Available)
# ---------------------------------------------------------
from dynamic_des.connectors.egress.local import ConsoleEgress
from dynamic_des.connectors.ingress.local import LocalIngress
from dynamic_des.core.environment import DynamicRealtimeEnvironment
from dynamic_des.core.registry import SimulationRegistry
from dynamic_des.core.sampler import Sampler
from dynamic_des.models.params import CapacityConfig, DistributionConfig, SimParameter
from dynamic_des.models.schemas import EventPayload, TelemetryPayload
from dynamic_des.resources.resource import DynamicResource
from dynamic_des.utils import time_to_seconds

try:
    __version__ = version("dynamic-des")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

logging.getLogger(__name__).addHandler(logging.NullHandler())

# Base exports that require no extra dependencies
__all__ = [
    "DynamicRealtimeEnvironment",
    "Sampler",
    "SimulationRegistry",
    "DynamicResource",
    "SimParameter",
    "DistributionConfig",
    "CapacityConfig",
    "LocalIngress",
    "ConsoleEgress",
    "EventPayload",
    "TelemetryPayload",
    "time_to_seconds",
]

# ---------------------------------------------------------
# Optional: Kafka Connectors (Requires `pip install dynamic-des[kafka]`)
# ---------------------------------------------------------
try:
    from dynamic_des.connectors.admin.kafka import KafkaAdminConnector  # noqa: F401
    from dynamic_des.connectors.egress.kafka import KafkaEgress  # noqa: F401
    from dynamic_des.connectors.ingress.kafka import KafkaIngress  # noqa: F401

    __all__.extend(["KafkaAdminConnector", "KafkaEgress", "KafkaIngress"])
except ImportError:
    pass

# ---------------------------------------------------------
# Optional: Storage Connectors (Requires `pip install dynamic-des[parquet]`)
# ---------------------------------------------------------
try:
    from dynamic_des.connectors.egress.storage import (
        JsonlStorageEgress,  # noqa: F401
        ParquetStorageEgress,  # noqa: F401
    )

    __all__.extend(["JsonlStorageEgress", "ParquetStorageEgress"])
except ImportError:
    pass

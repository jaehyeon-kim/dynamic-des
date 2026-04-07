import logging
from importlib.metadata import PackageNotFoundError, version

from dynamic_des.connectors.admin.kafka import KafkaAdminConnector
from dynamic_des.connectors.egress.kafka import KafkaEgress
from dynamic_des.connectors.egress.local import ConsoleEgress
from dynamic_des.connectors.ingress.kafka import KafkaIngress
from dynamic_des.connectors.ingress.local import LocalIngress
from dynamic_des.core.environment import DynamicRealtimeEnvironment
from dynamic_des.core.registry import SimulationRegistry
from dynamic_des.core.sampler import Sampler
from dynamic_des.models.params import CapacityConfig, DistributionConfig, SimParameter
from dynamic_des.models.schemas import EventPayload, TelemetryPayload
from dynamic_des.resources.resource import DynamicResource

try:
    __version__ = version("dynamic-des")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

logging.getLogger(__name__).addHandler(logging.NullHandler())


__all__ = [
    "DynamicRealtimeEnvironment",
    "Sampler",
    "SimulationRegistry",
    "DynamicResource",
    "SimParameter",
    "DistributionConfig",
    "CapacityConfig",
    "KafkaAdminConnector",
    "KafkaIngress",
    "LocalIngress",
    "KafkaEgress",
    "ConsoleEgress",
    "EventPayload",
    "TelemetryPayload",
]

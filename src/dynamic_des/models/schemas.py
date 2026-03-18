from typing import Any, Dict, Literal

from pydantic import BaseModel, Field


class BaseStreamPayload(BaseModel):
    """
    Base schema for all egress data emitted by the simulation engine.

    This schema guarantees that all outgoing data streams share a common temporal context,
    allowing downstream systems to accurately synchronize simulation time with real-world time.

    Attributes:
        stream_type (Literal["telemetry", "event"]): An identifier indicating the nature of the payload.
        sim_ts (float): The simulation clock time (in seconds) when the payload was generated.
        timestamp (str): The real-world ISO 8601 timestamp indicating when the payload was generated.
    """

    stream_type: Literal["telemetry", "event"]
    sim_ts: float = Field(..., description="The simulation clock time (in seconds).")
    timestamp: str = Field(..., description="The real-world ISO 8601 timestamp.")


class TelemetryPayload(BaseStreamPayload):
    """
    Schema for low-volume, single-metric telemetry updates.

    This payload is used for publishing continuous system state variables—such as resource
    utilization, queue lengths, or system lag—typically used for real-time dashboards.

    Attributes:
        stream_type (Literal["telemetry"]): Hardcoded to "telemetry" for downstream routing.
        path_id (str): The dot-notation path of the metric (e.g., 'Line_A.lathe.utilization').
        value (Any): The scalar value of the metric at the given simulation time.
    """

    stream_type: Literal["telemetry"] = "telemetry"
    path_id: str = Field(
        ...,
        description="The dot-notation path of the metric (e.g., 'Line_A.lathe.utilization').",
    )
    value: Any = Field(..., description="The scalar value of the metric.")


class EventPayload(BaseStreamPayload):
    """
    Schema for high-volume discrete simulation events.

    This payload tracks state transitions of specific entities (like tasks or parts) throughout
    the simulation lifecycle. The `key` attribute allows message brokers like Kafka to maintain
    strict chronological ordering for specific tasks.

    Attributes:
        stream_type (Literal["event"]): Hardcoded to "event" for downstream routing.
        key (str): A unique identifier/partition key for the event (e.g., 'task-001').
        value (Dict[str, Any]): A dictionary containing the event's detailed payload.
    """

    stream_type: Literal["event"] = "event"
    key: str = Field(
        ...,
        description="A unique identifier/partition key for the event (e.g., 'task-001').",
    )
    value: Dict[str, Any] = Field(
        ..., description="A dictionary containing the event payload."
    )

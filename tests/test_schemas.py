import pytest
from pydantic import ValidationError

from dynamic_des.models.schemas import EventPayload, TelemetryPayload


def test_telemetry_payload_valid():
    """Verify Pydantic successfully dumps valid telemetry data."""
    payload = TelemetryPayload(
        path_id="Line_A.utilization",
        value=85.5,
        sim_ts=10.5,
        timestamp="2023-10-25T14:30:00.000Z",
    )
    data = payload.model_dump(mode="json")

    assert data["stream_type"] == "telemetry"
    assert data["path_id"] == "Line_A.utilization"
    assert data["value"] == 85.5


def test_telemetry_payload_invalid():
    """Verify Pydantic blocks malformed telemetry data."""
    with pytest.raises(ValidationError):
        TelemetryPayload(
            path_id="Line_A.utilization",
            # missing value, sim_ts, timestamp
        )


def test_event_payload_valid():
    """Verify Pydantic successfully dumps valid event data."""
    payload = EventPayload(
        key="task-001",
        value={"status": "finished", "duration": 45.2},
        sim_ts=125.0,
        timestamp="2023-10-25T14:30:04.500Z",
    )
    data = payload.model_dump(mode="json")

    assert data["stream_type"] == "event"
    assert data["key"] == "task-001"
    assert data["value"]["status"] == "finished"

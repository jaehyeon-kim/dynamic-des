import asyncio
import queue

import pytest

from dynamic_des.connectors.egress.base import BaseEgress
from dynamic_des.core.environment import DynamicRealtimeEnvironment


class TrackingEgress(BaseEgress):
    """A mock egress provider that consumes the queue and tracks batches for testing."""

    def __init__(self):
        self.received_batches = []

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    # Actually consume the queue to prevent teardown deadlocks!
                    batch = egress_queue.get_nowait()
                    self.received_batches.append(batch)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass  # Clean shutdown


def test_environment_clean_teardown():
    """Verify that background threads and asyncio loops close cleanly."""
    env = DynamicRealtimeEnvironment(strict=False)
    tracker = TrackingEgress()
    env.setup_egress(providers=[tracker])

    env.run(until=1)

    try:
        env.teardown()
    except Exception as e:
        pytest.fail(f"Environment teardown raised an exception: {e}")


def test_publish_telemetry():
    """Verify telemetry formatting and queue placement."""
    env = DynamicRealtimeEnvironment(strict=False)
    tracker = TrackingEgress()

    # Set batch_size=1 so it flushes to the queue immediately
    env.setup_egress(providers=[tracker], batch_size=1)

    env.publish_telemetry("Line_A.lathe.utilization", 85.5)

    # Teardown guarantees the queue is fully drained into our tracker
    env.teardown()

    # The tracker should have received 1 batch containing 1 payload
    assert len(tracker.received_batches) == 1
    payload = tracker.received_batches[0][0]

    assert payload["stream_type"] == "telemetry"
    assert payload["path_id"] == "Line_A.lathe.utilization"
    assert payload["value"] == 85.5
    assert "sim_ts" in payload
    assert "timestamp" in payload


def test_publish_event():
    """Verify discrete event formatting and queue buffering."""
    env = DynamicRealtimeEnvironment(strict=False)
    tracker = TrackingEgress()

    env.setup_egress(providers=[tracker], batch_size=1)

    env.publish_event("task-001", {"status": "started"})

    env.teardown()

    assert len(tracker.received_batches) == 1
    payload = tracker.received_batches[0][0]

    assert payload["stream_type"] == "event"
    assert payload["key"] == "task-001"
    assert payload["value"] == {"status": "started"}

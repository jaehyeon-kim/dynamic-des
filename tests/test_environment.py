import asyncio
import queue

import pytest

from dynamic_des.connectors.egress.base import BaseEgress
from dynamic_des.core.environment import DynamicRealtimeEnvironment


class DummyEgress(BaseEgress):
    """A simple egress provider that sleeps infinitely to simulate an active network connection."""

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass  # Clean shutdown


def test_environment_clean_teardown():
    """Verify that background threads and asyncio loops close without throwing ValueErrors."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[DummyEgress()])

    env.run(until=1)

    try:
        env.teardown()
    except Exception as e:
        pytest.fail(f"Environment teardown raised an exception: {e}")


def test_publish_telemetry():
    """Verify telemetry formatting and queue placement."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[DummyEgress()])

    env.publish_telemetry("Line_A.lathe.utilization", 85.5)

    batch = env.egress_queue.get_nowait()
    assert len(batch) == 1
    payload = batch[0]

    assert payload["stream_type"] == "telemetry"
    assert payload["path_id"] == "Line_A.lathe.utilization"
    assert payload["value"] == 85.5
    assert "sim_ts" in payload
    assert "timestamp" in payload

    env.teardown()


def test_publish_event():
    """Verify discrete event formatting and queue buffering."""
    env = DynamicRealtimeEnvironment(strict=False)
    # Set batch_size=1 so it flushes immediately for the test
    env.setup_egress(providers=[DummyEgress()], batch_size=1)

    env.publish_event("task-001", {"status": "started"})

    batch = env.egress_queue.get_nowait()
    assert len(batch) == 1
    payload = batch[0]

    assert payload["stream_type"] == "event"
    assert payload["key"] == "task-001"
    assert payload["value"] == {"status": "started"}

    env.teardown()

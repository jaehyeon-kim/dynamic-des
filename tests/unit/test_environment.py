import asyncio
import queue
import time

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


class SlowEgress(BaseEgress):
    """An egress provider that consumes one batch at a time, slowly.

    The old teardown gave the queue five fixed seconds and then discarded the rest,
    so a sink like this lost its tail. These tests pin the behaviour that replaced
    that: the wait lasts as long as the queue keeps shrinking.
    """

    def __init__(self, delay: float = 0.2):
        self.delay = delay
        self.received_batches: list = []

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
                    continue
                await asyncio.sleep(self.delay)
                self.received_batches.append(batch)
        except asyncio.CancelledError:
            pass


class StalledEgress(BaseEgress):
    """An egress provider that never consumes anything."""

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass


def test_egress_queue_is_bounded():
    """A bounded queue is what turns a slow sink into backpressure."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[TrackingEgress()], max_queued_batches=7)

    assert env.egress_queue.maxsize == 7

    env.teardown()


def test_teardown_waits_for_a_slow_sink():
    """Teardown drains the queue rather than abandoning it on a deadline."""
    env = DynamicRealtimeEnvironment(strict=False)
    slow = SlowEgress(delay=0.2)
    env.setup_egress(providers=[slow], batch_size=1)

    for index in range(10):
        env.publish_telemetry(f"metric_{index}", float(index))

    env.teardown()

    # Ten batches at 0.2s each take longer than the five seconds the old code
    # allowed, so this is exactly the case that used to lose data.
    assert env.egress_queue.empty()
    assert len(slow.received_batches) == 10


def test_teardown_gives_up_when_the_queue_stops_shrinking():
    """A sink that consumes nothing must not hang teardown forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[StalledEgress()], batch_size=1, drain_stall_seconds=0.3)

    env.egress_queue.put([{"stream_type": "telemetry"}])

    env.teardown()

    assert not env.egress_queue.empty()


def test_flush_fails_loudly_when_the_sink_stops_consuming():
    """A full queue raises rather than blocking the simulation forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[StalledEgress()],
        batch_size=1,
        max_queued_batches=1,
        drain_stall_seconds=0.2,
    )

    env.egress_queue.put(["already full"])

    with pytest.raises(RuntimeError, match="stopped consuming"):
        env.publish_telemetry("metric", 1.0)

    env.teardown()


class BusyProvider(BaseEgress):
    """A provider that reports work in flight without ever finishing it."""

    def __init__(self):
        self.active_tasks = 3

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def test_egress_defaults_are_bounded_and_patient():
    """The defaults are the contract, so they are worth pinning."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[TrackingEgress()])

    assert env.egress_queue.maxsize == 2000
    assert env.egress_drain_stall_seconds == 30.0

    env.teardown()


def test_backpressure_loses_nothing_with_a_tiny_queue():
    """A queue far smaller than the workload must still deliver every batch."""
    env = DynamicRealtimeEnvironment(strict=False)
    slow = SlowEgress(delay=0.02)
    env.setup_egress(providers=[slow], batch_size=1, max_queued_batches=2)

    for index in range(25):
        env.publish_telemetry(f"metric_{index}", float(index))

    env.teardown()

    assert len(slow.received_batches) == 25
    assert env.egress_queue.empty()


def test_teardown_returns_when_providers_never_finish():
    """A provider stuck with active tasks must not hold teardown open forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[BusyProvider()], drain_stall_seconds=0.3)

    started = time.monotonic()
    env.teardown()
    elapsed = time.monotonic() - started

    # The guard gives up shortly after the stall window rather than hanging.
    assert elapsed < 5.0


def test_zero_stall_window_does_not_hang():
    """An impatient setting is still a working setting."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[StalledEgress()], batch_size=1, drain_stall_seconds=0.0)

    env.egress_queue.put([{"stream_type": "telemetry"}])

    started = time.monotonic()
    env.teardown()
    assert time.monotonic() - started < 5.0


def test_stall_window_is_honoured_before_giving_up():
    """The wait lasts about as long as it was told to, not five fixed seconds."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[StalledEgress()], batch_size=1, drain_stall_seconds=1.0)

    env.egress_queue.put([{"stream_type": "telemetry"}])

    started = time.monotonic()
    env.teardown()
    elapsed = time.monotonic() - started

    assert elapsed >= 1.0
    assert elapsed < 6.0

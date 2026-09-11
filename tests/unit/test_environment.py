import asyncio
import logging
import queue
import time
from datetime import datetime, timedelta, timezone

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


class RecordingEgress:
    """Captures every record it is handed, so a test can compare sinks."""

    def __init__(self):
        self.records: list = []
        self.batch_sizes: list = []
        self.active_tasks = 0

    async def run(self, egress_queue: queue.Queue):
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.records.extend(batch)
                self.batch_sizes.append(len(batch))
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_two_providers_each_receive_every_record():
    """Fan-out. Providers used to share one queue and compete for batches, so a run
    with two sinks wrote part of the data to each with no error."""
    first, second = RecordingEgress(), RecordingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[first, second], batch_size=1)

    for i in range(5):
        env.publish_telemetry("metric", i)

    env.teardown()

    assert len(first.records) == 5
    assert len(second.records) == 5
    assert [r["value"] for r in first.records] == [r["value"] for r in second.records]


def test_a_predicate_routes_records_to_one_provider():
    """Per-record routing. Each provider takes an optional predicate."""
    hot, cold = RecordingEgress(), RecordingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[hot, cold],
        batch_size=1,
        predicates=[
            lambda r: r.get("value", 0) >= 3,
            lambda r: r.get("value", 0) < 3,
        ],
    )

    for i in range(5):
        env.publish_telemetry("metric", i)

    env.teardown()

    assert sorted(r["value"] for r in hot.records) == [3, 4]
    assert sorted(r["value"] for r in cold.records) == [0, 1, 2]


def test_predicate_count_must_match_provider_count():
    """A misaligned list would silently route the wrong records, so it is rejected."""
    env = DynamicRealtimeEnvironment(strict=False)
    with pytest.raises(ValueError, match="matched by position"):
        env.setup_egress(
            providers=[RecordingEgress(), RecordingEgress()], predicates=[None]
        )


LOGICAL_START = datetime(2024, 1, 1, 12, 0, 0)


def watch_factor(env, until, interval=0.25):
    """Runs `env` and returns the pacing factor seen at each simulated instant."""
    seen: list = []

    def watcher():
        while True:
            seen.append((round(env.now, 3), env.factor))
            yield env.timeout(interval)

    env.process(watcher())
    env.run(until=until)
    return seen


def test_factor_switches_when_the_logical_clock_reaches_go_live():
    """One run, two speeds: unpaced history, then real time from the go-live instant."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=0.5),
    )

    started = time.monotonic()
    seen = watch_factor(env, until=1.0)
    elapsed = time.monotonic() - started

    before = [factor for sim_time, factor in seen if sim_time < 0.5]
    after = [factor for sim_time, factor in seen if sim_time >= 0.5]

    assert before and set(before) == {0.0}
    assert len(after) >= 2 and set(after) == {1.0}

    # The half second after the switch is paced, so it costs half a second of real
    # time. Without the switch the whole run would finish immediately.
    assert elapsed >= 0.4
    assert elapsed < 5.0


def test_live_pacing_is_measured_from_the_go_live_instant():
    """Backfilled simulation time must not become real waiting at the switch.

    simpy maps simulation time onto the wall clock from a pair of anchors taken when
    the environment is built. Switching the factor without moving those anchors makes
    the first paced event sleep off the entire backfill, which here would be ten
    seconds instead of a quarter of a second, and a week in the case this exists for.
    """
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=10),
    )

    seen: list = []

    def backfill_then_live():
        yield env.timeout(9.5)  # the whole backfill in one step
        seen.append((env.now, env.factor))
        yield env.timeout(0.5)  # lands exactly on the go-live instant
        seen.append((env.now, env.factor))
        yield env.timeout(0.25)  # the first paced interval
        seen.append((env.now, env.factor))

    env.process(backfill_then_live())

    started = time.monotonic()
    env.run()
    elapsed = time.monotonic() - started

    assert [factor for _, factor in seen] == [0.0, 1.0, 1.0]
    assert elapsed >= 0.2
    assert elapsed < 5.0


def test_a_run_without_go_live_keeps_its_factor():
    """No go-live instant means the previous behaviour, unpaced from start to end."""
    env = DynamicRealtimeEnvironment(
        factor=0.0, strict=False, logical_start_time=LOGICAL_START
    )

    started = time.monotonic()
    seen = watch_factor(env, until=3600, interval=60)
    elapsed = time.monotonic() - started

    assert {factor for _, factor in seen} == {0.0}
    assert env.factor == 0.0
    # An hour of simulated time still costs no real time, and the clock anchor that
    # the switch would have moved is untouched.
    assert elapsed < 2.0
    assert env.env_start == 0


def test_go_live_before_the_start_paces_the_whole_run(caplog):
    """A go-live instant already in the past is the degenerate case: live throughout."""
    with caplog.at_level(logging.WARNING, logger="dynamic_des.core.environment"):
        env = DynamicRealtimeEnvironment(
            factor=0.0,
            strict=False,
            logical_start_time=LOGICAL_START,
            go_live_at=LOGICAL_START - timedelta(hours=1),
        )

    seen = watch_factor(env, until=0.5)

    assert set(factor for _, factor in seen) == {1.0}
    assert "before the start of the run" in caplog.text


def test_go_live_at_the_start_is_a_live_run():
    """Going live at the first instant is allowed and needs no warning."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START,
    )

    seen = watch_factor(env, until=0.5)

    assert set(factor for _, factor in seen) == {1.0}


def test_go_live_does_not_hold_the_schedule_open():
    """A pending switch must not keep an otherwise finished run alive."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(days=7),
    )

    def short_process():
        yield env.timeout(1.0)

    env.process(short_process())

    started = time.monotonic()
    env.run()
    elapsed = time.monotonic() - started

    assert env.now == 1.0
    assert env.factor == 0.0
    assert elapsed < 2.0


def test_go_live_rejects_a_clock_it_cannot_compare():
    """A naive start and an aware go-live have no measurable interval between them."""
    with pytest.raises(ValueError, match="both be naive"):
        DynamicRealtimeEnvironment(
            factor=0.0,
            strict=False,
            logical_start_time=LOGICAL_START,
            go_live_at=datetime.now(tz=timezone.utc),
        )


def test_batch_size_governs_when_unpaced():
    """At factor=0 the simulation clock outruns the wall clock, so a flush timer in
    simulation seconds fires constantly and batch_size never governs. Issue #25."""
    tracker = RecordingEgress()
    env = DynamicRealtimeEnvironment(factor=0, strict=False)
    # lag_monitor_interval=0 turns off the built-in lag metric, which would otherwise
    # publish one record per simulated second and change the batch arithmetic.
    env.setup_egress(
        providers=[tracker], batch_size=10, flush_interval=1.0, lag_monitor_interval=0
    )

    def publisher():
        for i in range(30):
            env.publish_telemetry("metric", i)
            yield env.timeout(1.0)

    env.process(publisher())
    env.run(until=31)
    env.teardown()

    assert len(tracker.records) == 30
    # 30 records at batch_size 10 is three full batches. A per-simulation-second timer
    # would have cut them into batches of one.
    assert tracker.batch_sizes[:3] == [10, 10, 10]


def test_flush_timer_still_runs_when_paced():
    """With factor above 0 the interval is a real latency bound, so it stays."""
    tracker = RecordingEgress()
    env = DynamicRealtimeEnvironment(factor=0.001, strict=False)
    env.setup_egress(
        providers=[tracker], batch_size=1000, flush_interval=1.0, lag_monitor_interval=0
    )

    def publisher():
        for i in range(5):
            env.publish_telemetry("metric", i)
            yield env.timeout(1.0)

    env.process(publisher())
    env.run(until=6)
    env.teardown()

    # batch_size is never reached, so every record arrived via the timer.
    assert len(tracker.records) == 5
    assert max(tracker.batch_sizes) < 1000

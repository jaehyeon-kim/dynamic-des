import asyncio
import enum
import logging
import queue
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from decimal import Decimal

import numpy as np
import orjson
import pytest
from pydantic import BaseModel, Field, ValidationError
from pydantic_core import PydanticSerializationError

from dynamic_des.connectors.egress.base import BaseEgress
from dynamic_des.core.environment import DynamicRealtimeEnvironment
from dynamic_des.models.schemas import EventPayload, TelemetryPayload


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


class _Level(enum.IntEnum):
    HIGH = 9


class _Aliased(BaseModel):
    """A model whose field renames itself on the way out."""

    real_name: str = Field(serialization_alias="aliasName")


class _Nested(BaseModel):
    when: datetime
    amount: Decimal


# Values chosen because each one is serialized by a different branch of the
# pydantic serializer. Every one of them is published by both the model and the
# hand-built dict, so the two must agree on all of them.
PAYLOAD_VALUES = [
    ("json_native", {"s": "a", "i": 1, "f": 1.5, "b": True, "n": None}),
    ("nested", {"a": {"b": [1, 2, {"c": "d"}]}}),
    ("empty", {}),
    ("datetime", {"when": datetime(2024, 3, 4, 5, 6, 7, 891234)}),
    (
        "date_time_delta",
        {"d": date(2024, 3, 4), "t": dt_time(1, 2, 3), "td": timedelta(days=1)},
    ),
    ("decimal", {"amount": Decimal("1.10")}),
    ("uuid", {"id": uuid.UUID("12345678-1234-5678-1234-567812345678")}),
    ("enum", {"level": _Level.HIGH}),
    ("set_frozenset", {"s": {1, 2}, "f": frozenset(["a"])}),
    ("bytes", {"b": b"abc"}),
    ("model", _Nested(when=datetime(2024, 1, 1), amount=Decimal("2.50"))),
    ("model_in_dict", {"n": _Nested(when=datetime(2024, 1, 1), amount=Decimal("0.1"))}),
    ("aliased_model", _Aliased(real_name="n")),
    ("nan_inf", {"a": float("nan"), "b": float("inf"), "c": float("-inf")}),
    ("bare_nan", float("nan")),
    ("big_int", {"n": 2**80}),
    ("scalar_str", "a bare string"),
    ("scalar_int", 42),
    ("scalar_list", [1, "two", 3.0, None]),
    (
        "envelope_key_collision",
        {"stream_type": "x", "timestamp": "y", "key": "z", "sim_ts": 1},
    ),
    ("numpy_float", {"v": np.float64(2.5)}),
]


def _buffered_env():
    """An environment whose records stay in the buffer, so each one can be read."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[TrackingEgress()], batch_size=10**6, lag_monitor_interval=0
    )
    return env


def _assert_same_payload(record, expected, case_id):
    assert record == expected, case_id
    assert list(record) == list(expected), f"{case_id}: key order"
    assert [type(v) for v in record.values()] == [type(v) for v in expected.values()], (
        f"{case_id}: value types"
    )


@pytest.mark.parametrize(
    "case_id,value", PAYLOAD_VALUES, ids=[c[0] for c in PAYLOAD_VALUES]
)
def test_publish_event_matches_event_payload_model(case_id, value):
    """publish_event builds its dict directly, so pin it to the schema it replaced.

    Up to 0.12.0 the record was `EventPayload(...).model_dump(mode="json")`. Consumers
    read that shape, so the hand-built dict has to reproduce it byte for byte, not
    merely decode to the same JSON.
    """
    env = _buffered_env()
    try:
        expected = EventPayload(
            key="task-001",
            value=value,
            sim_ts=round(env.now, 3),
            timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
        ).model_dump(mode="json")

        env.publish_event("task-001", value)

        _assert_same_payload(env._event_buffer[-1], expected, case_id)
    finally:
        env.teardown()


@pytest.mark.parametrize(
    "case_id,value", PAYLOAD_VALUES, ids=[c[0] for c in PAYLOAD_VALUES]
)
def test_publish_telemetry_matches_telemetry_payload_model(case_id, value):
    """Same contract as publish_event, for the scalar metric stream."""
    env = _buffered_env()
    try:
        expected = TelemetryPayload(
            path_id="Line_A.lathe.utilization",
            value=value,
            sim_ts=round(env.now, 3),
            timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
        ).model_dump(mode="json")

        env.publish_telemetry("Line_A.lathe.utilization", value)

        _assert_same_payload(env._event_buffer[-1], expected, case_id)
    finally:
        env.teardown()


def test_sim_ts_is_a_float_when_the_clock_is_an_integer():
    """`env.now` is the integer 0 until the first fractional timeout.

    `sim_ts` was declared `float` on the model, so 0 was published as 0.0 and every
    JSON record carried "sim_ts":0.0. Publishing the raw integer writes "sim_ts":0,
    which changes the bytes and the inferred column type of every early record.
    """
    env = _buffered_env()
    try:
        assert isinstance(env.now, int)

        env.publish_event("task-001", {"status": "started"})
        env.publish_telemetry("metric", 1.0)

        for record in env._event_buffer:
            assert type(record["sim_ts"]) is float
        assert b'"sim_ts":0.0' in orjson.dumps(env._event_buffer[0])
    finally:
        env.teardown()


def test_sim_ts_is_a_plain_float_when_the_clock_is_a_numpy_float():
    """A numpy delay leaves `env.now` a numpy float, which orjson cannot serialize.

    The model coerced it to a plain float, so the sinks never saw a numpy type.
    """
    env = _buffered_env()
    try:
        env._now = np.float64(3.14159)

        env.publish_event("task-001", {"status": "started"})

        record = env._event_buffer[-1]
        assert type(record["sim_ts"]) is float
        assert record["sim_ts"] == 3.142
        assert orjson.dumps(record)  # a numpy float raises here
    finally:
        env.teardown()


def test_non_string_keys_are_coerced_or_rejected_as_the_model_did():
    """`key` and `path_id` were declared `str`, so the model validated them.

    Bytes were decoded and a number was rejected outright. Passing them straight
    through instead wrote a non-string key to the sinks, and orjson cannot serialize
    bytes at all.
    """
    env = _buffered_env()
    try:
        env.publish_event(b"task-001", {"status": "started"})
        assert env._event_buffer[-1]["key"] == "task-001"
        assert type(env._event_buffer[-1]["key"]) is str

        env.publish_telemetry(b"metric", 1.0)
        assert env._event_buffer[-1]["path_id"] == "metric"

        with pytest.raises(ValidationError):
            env.publish_event(5, {"status": "started"})
        with pytest.raises(ValidationError):
            env.publish_telemetry(None, 1.0)
    finally:
        env.teardown()


def test_serialization_alias_is_not_applied_to_the_value():
    """`model_dump(mode="json")` defaults to by_alias=False, to_jsonable_python to True.

    Left at its default the new path renamed every aliased field of a user model, so
    a consumer reading `real_name` would have found `aliasName` instead.
    """
    env = _buffered_env()
    try:
        env.publish_event("task-001", _Aliased(real_name="n"))
        assert env._event_buffer[-1]["value"] == {"real_name": "n"}
    finally:
        env.teardown()


def test_non_finite_floats_become_null():
    """`model_dump(mode="json")` writes None for NaN and infinity.

    to_jsonable_python defaults to leaving them as floats, which a Parquet sink
    stores as NaN rather than null.
    """
    env = _buffered_env()
    try:
        env.publish_telemetry("metric", float("nan"))
        env.publish_telemetry("metric", float("inf"))
        env.publish_event("task-001", {"a": float("-inf")})

        assert env._event_buffer[0]["value"] is None
        assert env._event_buffer[1]["value"] is None
        assert env._event_buffer[2]["value"] == {"a": None}
    finally:
        env.teardown()


def test_a_value_the_serializer_cannot_handle_still_raises():
    """The model refused a type it could not serialize, and so must this path.

    Letting the object through would move the failure to the egress thread, where the
    record is lost rather than reported.
    """
    env = _buffered_env()
    try:
        with pytest.raises(PydanticSerializationError):
            env.publish_event("task-001", {"i": np.int64(7)})
        with pytest.raises(PydanticSerializationError):
            env.publish_telemetry("metric", object())
    finally:
        env.teardown()


def test_envelope_key_order_matches_the_published_schema():
    """Key order is part of the record, because the sinks serialize the dict as given."""
    env = _buffered_env()
    try:
        env.publish_event("task-001", {"status": "started"})
        env.publish_telemetry("metric", 1.0)

        assert list(env._event_buffer[0]) == [
            "stream_type",
            "sim_ts",
            "timestamp",
            "key",
            "value",
        ]
        assert list(env._event_buffer[1]) == [
            "stream_type",
            "sim_ts",
            "timestamp",
            "path_id",
            "value",
        ]
        assert list(EventPayload.model_fields) == list(env._event_buffer[0])
        assert list(TelemetryPayload.model_fields) == list(env._event_buffer[1])
    finally:
        env.teardown()

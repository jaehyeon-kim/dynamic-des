import asyncio
import logging
import math
import queue
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python

from simpy import RealtimeEnvironment

from dynamic_des.core.registry import SimulationRegistry

logger = logging.getLogger(__name__)

# EventPayload.key and TelemetryPayload.path_id are declared `str`, so building the
# model coerced them: bytes were decoded and an int was rejected with a
# ValidationError. Building the dict directly skipped that, which let a non-string
# key reach the sinks. This is the validator those fields used, so it agrees by
# construction rather than by imitation.
_KEY_ADAPTER = TypeAdapter(str)


def _jsonable(value: Any) -> Any:
    """Serializes `value` exactly as `model_dump(mode="json")` did in 0.12.0.

    Two `to_jsonable_python` defaults disagree with the model dump, so both are set
    back. `by_alias` defaults to True here and to False in the dump, which renamed
    every field of a model that declares a serialization alias. `inf_nan_mode`
    defaults to "constants" here and to "null" in the dump, which let a NaN or an
    infinity through as a float instead of None.
    """
    return to_jsonable_python(value, by_alias=False, inf_nan_mode="null")


# Pacing a run takes after `go_live_at`. It is fixed rather than configurable because
# `go_live_at` names a moment, not a speed: going live means one simulated second per
# real second. A second speed setting would be a general scheduled change of `factor`,
# which is a different feature.
LIVE_FACTOR = 1.0

# A timer flush holding less than this share of `batch_size` is one where the size
# did not govern. A quarter is low enough that ordinary variation does not count.
_STARVED_BATCH_RATIO = 0.25
# How many of those in a row before saying so. High enough that a slow opening or a
# brief lull is not mistaken for a misconfiguration.
_STARVED_FLUSHES_BEFORE_WARNING = 20


class RegistryMixIn:
    """
    MixIn to attach a central SimulationRegistry to the environment.
    """

    def setup_registry(self):
        """Initializes the SimulationRegistry and binds it to the environment."""
        self.registry = SimulationRegistry(self)  # type: ignore[arg-type]


class IngressMixIn:
    """
    Handles background I/O for Ingress (Incoming updates).

    This MixIn manages a background thread and an asyncio event loop to
    continuously poll external sources (like Kafka or Redis) for state changes
    without blocking the main SimPy execution loop.
    """

    def setup_ingress(self, providers: List):
        """
        Initializes the ingress queues and starts the background listener threads.

        Args:
            providers (List[BaseIngress]): A list of initialized ingress connector instances.
        """
        self.ingress_queue: queue.Queue[Any] = queue.Queue()
        self.ingress_providers = providers
        self._ingress_loop: Optional[asyncio.AbstractEventLoop] = (
            None  # Store loop reference
        )
        self._ingress_thread = threading.Thread(
            target=self._run_ingress_loop, daemon=True
        )
        self._ingress_thread.start()
        self.process(self._ingress_monitor())  # type: ignore[attr-defined]

    def _run_ingress_loop(self):
        """Internal: Runs the asyncio event loop for ingress providers in a background thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._ingress_loop = loop

        # Keep a list of the running tasks
        tasks = [
            loop.create_task(provider.run(self.ingress_queue))
            for provider in self.ingress_providers
        ]

        try:
            # Run the loop until teardown() calls loop.stop()
            loop.run_forever()
        finally:
            # Cancel all pending tasks
            for task in tasks:
                task.cancel()
            # Briefly run the loop again to let the CancelledError propagate cleanly
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # Safely close the loop
            loop.close()

    def _ingress_monitor(self):
        """Internal: A SimPy process that checks the thread-safe queue for new data."""
        while True:
            while True:
                try:
                    path, value = self.ingress_queue.get_nowait()
                    logger.debug(f"Ingress update received: {path} = {value}")
                    self.registry.update(path, value)  # type: ignore[attr-defined]
                except queue.Empty:
                    break  # Queue is empty, exit the inner loop
            yield self.timeout(0.1)  # type: ignore[attr-defined]

    def teardown_ingress(self):
        """Safely stops the background ingress event loop."""
        logger.info("Tearing down ingress connectors...")
        loop = getattr(self, "_ingress_loop", None)
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)


class EgressMixIn:
    """
    Handles high-throughput data egress to external systems.

    This MixIn manages a background thread to asynchronously push telemetry
    and event data to destinations like Kafka, Redis, or PostgreSQL. It uses
    a buffered approach to maximize throughput.
    """

    def setup_egress(
        self,
        providers: List,
        batch_size: int = 500,
        flush_interval: float = 1.0,
        lag_monitor_interval: Optional[float] = 1.0,
        max_queued_batches: int = 2000,
        drain_stall_seconds: float = 30.0,
        predicates: Optional[List[Optional[Callable[[Dict[str, Any]], bool]]]] = None,
        batch_sizes: Optional[List[Optional[int]]] = None,
        flush_intervals: Optional[List[Optional[float]]] = None,
    ):
        """
        Initializes the egress buffers and starts the background publisher threads.

        Args:
            providers (List[BaseEgress]): A list of initialized egress connector instances.
            batch_size (int, optional): The maximum number of events to buffer before flushing. Defaults to 500.
            flush_interval (float, optional): Maximum simulation seconds to wait before
                forcing a flush. Defaults to 1.0. Ignored when `factor` is 0, because
                simulation time is then detached from the wall clock and the interval
                bounds nothing, so `batch_size` alone decides when a batch is sent.
            max_queued_batches (int, optional): Upper bound on batches waiting for the
                egress threads. A full queue blocks the producer, so a sink that cannot
                keep up slows the simulation rather than accumulating a backlog that
                teardown would have to discard. Defaults to 2000.
            drain_stall_seconds (float, optional): At teardown every queue is drained
                until empty. The wait is abandoned only if a queue stops shrinking for
                this long, so a slow sink finishes rather than losing its tail.
                Defaults to 30.0.
            predicates (List, optional): One entry per provider, aligned by position.
                A callable takes a record and returns True to send it to that provider.
                None means that provider receives every record. Defaults to None, which
                is every provider receiving everything.
            batch_sizes (List, optional): One entry per provider, aligned by position.
                None falls back to `batch_size`. A stream sink wants a small value and
                a lake sink a large one, because the batch is the file and, for
                Iceberg, the commit.
            flush_intervals (List, optional): One entry per provider, aligned by
                position. None falls back to `flush_interval`.

        Each provider gets its own queue. They previously shared one, so several
        providers competed for batches instead of each receiving them, and a run with
        two sinks wrote part of the data to each with no error and exit code 0.
        """
        self.egress_drain_stall_seconds = drain_stall_seconds
        self.egress_providers = providers
        self.egress_queues: List[queue.Queue[Any]] = [
            queue.Queue(maxsize=max_queued_batches) for _ in providers
        ]
        self.egress_predicates: List[Optional[Callable[[Dict[str, Any]], bool]]] = (
            list(predicates) if predicates else [None] * len(providers)
        )
        if len(self.egress_predicates) != len(providers):
            raise ValueError(
                f"predicates has {len(self.egress_predicates)} entries for "
                f"{len(providers)} providers; they are matched by position"
            )
        # The fallback a provider gets when it names no value of its own. Read
        # egress_batch_sizes and egress_flush_intervals for what each one actually
        # uses, since either can differ from this.
        self.egress_batch_size = batch_size
        self.egress_flush_interval = flush_interval
        self.egress_batch_sizes: List[int] = self._resolve_per_provider(
            batch_sizes, batch_size, len(providers), "batch_sizes"
        )
        self.egress_flush_intervals: List[float] = self._resolve_per_provider(
            flush_intervals, flush_interval, len(providers), "flush_intervals"
        )
        self.egress_lag_monitor_interval = lag_monitor_interval
        # One buffer per provider. They shared one until batching moved behind the
        # fan-out, which meant a single cadence for every sink and no way to give a
        # stream sink low latency while a lake sink built large files.
        self._event_buffers: List[List[Dict[str, Any]]] = [[] for _ in providers]
        # Counts consecutive timer-driven flushes that came in far below batch_size,
        # so a configuration where the size never governs is reported once per run.
        self._starved_flushes: List[int] = [0] * len(providers)
        self._starved_totals: List[int] = [0] * len(providers)
        self._starvation_warned: List[bool] = [False] * len(providers)
        self._egress_loop: Optional[asyncio.AbstractEventLoop] = (
            None  # Store loop reference
        )

        # Start background threads for providers
        self._egress_thread = threading.Thread(
            target=self._run_egress_loop, daemon=True
        )
        self._egress_thread.start()

        # Start background processes.
        # The periodic flush waits in simulation time, so at factor=0 it fires as fast
        # as the machine advances the clock and batch_size never governs. A 5,000
        # second run with batch_size=500 produced about 5,000 batches of 33 records
        # instead of 330 of 500, measured in issue #5. There is no wall clock to bound
        # latency against when factor is 0, so the timer has nothing to achieve and is
        # not started. Teardown flushes whatever is left in the buffer.
        self._periodic_flush_running = bool(getattr(self, "factor", 1.0))
        if self._periodic_flush_running:
            for index in range(len(providers)):
                self.process(self._periodic_flush(index))  # type: ignore[attr-defined]
        # Only start the lag monitor if an interval > 0 is provided
        if self.egress_lag_monitor_interval and self.egress_lag_monitor_interval > 0:
            self.process(self._lag_monitor())  # type: ignore[attr-defined]

    @property
    def _event_buffer(self) -> List[Dict[str, Any]]:
        """The first provider's buffer, kept so single-sink callers and tests still read.

        Each provider now buffers separately in `_event_buffers`, so that a stream
        sink and a lake sink can flush at different sizes. Read `_event_buffers` when
        the number of providers matters.
        """
        return self._event_buffers[0]

    @property
    def egress_queue(self) -> "queue.Queue[Any]":
        """The first provider's queue, kept so single-sink callers and tests still read.

        Each provider now has its own queue in `egress_queues`. With one provider the
        two are the same thing. With several, this returns only the first, so use
        `egress_queues` when the number of providers matters.
        """
        return self.egress_queues[0]

    def _lag_monitor(self):
        """Internal: Monitors how far the simulation time has drifted from the real-world clock."""
        while True:
            # Calculate real seconds elapsed since simulation start
            real_elapsed = (datetime.now() - self.start_datetime).total_seconds()  # type: ignore[attr-defined]
            lag = max(0.0, real_elapsed - self.now)  # type: ignore[attr-defined]

            # Publish this system health metric automatically
            self.publish_telemetry("system.simulation.lag_seconds", round(lag, 3))

            # Yield based on the configurable interval
            yield self.timeout(self.egress_lag_monitor_interval)  # type: ignore[attr-defined]

    def _run_egress_loop(self):
        """Internal: Runs the asyncio event loop for egress providers in a background thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._egress_loop = loop

        # Keep a list of the running tasks
        # One queue per provider, so every provider sees every batch routed to it.
        # Sharing one queue made them competing consumers of the same batches.
        tasks = [
            loop.create_task(provider.run(provider_queue))
            for provider, provider_queue in zip(
                self.egress_providers, self.egress_queues
            )
        ]

        try:
            # Run the loop until teardown() calls loop.stop()
            loop.run_forever()
        finally:
            # Cancel all pending tasks
            for task in tasks:
                task.cancel()
            # Briefly run the loop again to let the CancelledError propagate cleanly
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # Safely close the loop
            loop.close()

    @staticmethod
    def _resolve_per_provider(values, default, count, name):
        """Internal: fill per-provider overrides, falling back to the shared default."""
        if values is None:
            return [default] * count
        if len(values) != count:
            raise ValueError(
                f"{name} has {len(values)} entries for {count} providers; "
                "they are matched by position"
            )
        return [default if value is None else value for value in values]

    def _periodic_flush(self, index: int):
        """Internal: SimPy process flushing one provider's buffer on its own interval."""
        while True:
            yield self.timeout(self.egress_flush_intervals[index])  # type: ignore[attr-defined]
            self._note_timer_flush(index)
            self._flush_buffer(index)

    def _note_timer_flush(self, index: int):
        """Internal: warn once when `batch_size` never governs for a provider.

        The two limits are an OR, so the effective batch is the smaller of
        `batch_size` and what arrives within `flush_interval`. A high size with a
        short interval means the size does nothing, and no configuration-time check
        can catch it because the arrival rate is unknown until the run starts.
        Consecutive rather than cumulative, so a quiet period does not trip it.
        """
        if self._starvation_warned[index]:
            return
        pending = len(self._event_buffers[index])
        target = self.egress_batch_sizes[index]
        if pending and pending < target * _STARVED_BATCH_RATIO:
            self._starved_flushes[index] += 1
            self._starved_totals[index] += pending
            if self._starved_flushes[index] >= _STARVED_FLUSHES_BEFORE_WARNING:
                average = self._starved_totals[index] / self._starved_flushes[index]
                logger.warning(
                    "%s (provider %d) flushed on its timer %d times in a row "
                    "averaging %.0f records against a batch_size of %d, so batch_size "
                    "never governs for this sink. Raise flush_interval or lower "
                    "batch_size.",
                    type(self.egress_providers[index]).__name__,
                    index,
                    self._starved_flushes[index],
                    average,
                    target,
                )
                self._starvation_warned[index] = True
        else:
            self._starved_flushes[index] = 0
            self._starved_totals[index] = 0

    def _flush_buffer(self, index: Optional[int] = None):
        """Internal: Pushes one provider's buffered data to its thread-safe queue.

        With no index every provider is flushed, which is what teardown needs.

        The queue is bounded, so a sink that cannot keep up slows the simulation
        rather than accumulating a backlog. A sink that has stopped consuming
        altogether would otherwise block here forever, so the wait is capped and
        the failure is raised instead of hidden.
        """
        if index is None:
            for position in range(len(self._event_buffers)):
                self._flush_buffer(position)
            return

        batch = self._event_buffers[index]
        if not batch:
            return

        logger.debug("Flushing %d events to egress provider %d.", len(batch), index)
        stall_limit = getattr(self, "egress_drain_stall_seconds", 30.0)

        # The predicate was applied on the way in, so the buffer already holds only
        # what this provider takes.
        try:
            self.egress_queues[index].put(batch, timeout=stall_limit)
        except queue.Full:
            logger.error(
                "An egress queue has been full for %.0fs, so that sink has stopped "
                "consuming. Stopping rather than discarding events.",
                stall_limit,
            )
            raise RuntimeError(
                "Egress queue full: the sink stopped consuming events"
            ) from None

        self._event_buffers[index] = []

    def _append_record(self, record: Dict[str, Any]):
        """Internal: routes one record into each provider's buffer.

        Predicates are tested here rather than at flush time, so a record is tested
        once on the way in instead of against every predicate at fan-out.
        """
        for index, predicate in enumerate(self.egress_predicates):
            if predicate is not None and not predicate(record):
                continue
            self._event_buffers[index].append(record)
            if len(self._event_buffers[index]) >= self.egress_batch_sizes[index]:
                self._flush_buffer(index)

    def publish_telemetry(self, path_id: str, value: Any):
        """
        Publishes a low-volume telemetry metric (e.g., utilization, queue length).

        Telemetry shares the main event buffer to ensure efficient batching for
        high-throughput file storage (like Parquet or S3).
        """
        if not hasattr(self, "egress_queues"):
            return  # Fail silently if no egress is configured

        # Built directly rather than through TelemetryPayload. See publish_event.
        self._append_record(
            {
                "stream_type": "telemetry",
                "sim_ts": float(round(self.now, 3)),  # type: ignore[attr-defined]
                "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
                "path_id": (
                    path_id
                    if type(path_id) is str
                    else _KEY_ADAPTER.validate_python(path_id)
                ),
                "value": _jsonable(value),
            }
        )

    def publish_event(self, event_key: str, value: Any):
        """
        Buffers a high-volume discrete event (e.g., a task starting or finishing).

        Events are buffered and flushed either when `batch_size` is reached
        or when `flush_interval` occurs to optimize external I/O throughput.

        Args:
            event_key (str): A unique identifier for the event (e.g., 'task-001').
            value (Any): A dictionary or a Pydantic model containing the event
                payload. It is made JSON-serializable on the way out, so a nested model
                is flattened and a datetime becomes an ISO string, exactly as before.
        """
        if not hasattr(self, "_event_buffers"):
            return  # Fail silently if no egress is configured

        # Built directly rather than through EventPayload.model_dump(mode="json").
        # Constructing and dumping the model costs 1,790 ns per record against 980 ns
        # for this dict, on a four-field event. About 550 ns of each is the timestamp
        # formatting below, which both paths run.
        #
        # Every coercion the model performed is reproduced here, because each one
        # changed what the sinks received. `sim_ts` was declared `float`, so an integer
        # simulation clock was published as 0.0 rather than 0, and a numpy float was
        # published as a plain float that orjson can serialize. `key` was declared
        # `str`. `value` goes through `_jsonable`, which is `to_jsonable_python` with
        # the two defaults that disagree with the model dump set back. Keys and their
        # order match what the model produced. EventPayload remains the published
        # schema and is what the Avro and documentation paths describe.
        self._append_record(
            {
                "stream_type": "event",
                "sim_ts": float(round(self.now, 3)),  # type: ignore[attr-defined]
                "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
                "key": (
                    event_key
                    if type(event_key) is str
                    else _KEY_ADAPTER.validate_python(event_key)
                ),
                "value": _jsonable(value),
            }
        )

    def _get_iso_timestamp(self, start_time: datetime, sim_now: float) -> str:
        """Internal: Converts simulation time to a real-world ISO string."""
        return (start_time + timedelta(seconds=sim_now)).isoformat(
            timespec="milliseconds"
        )

    def teardown_egress(self):
        """Flushes final buffer contents and safely stops the egress event loop."""
        logger.info("Tearing down egress connectors, flushing final events...")
        if hasattr(self, "_event_buffers"):
            try:
                self._flush_buffer()
            except RuntimeError:
                # A sink that has stopped consuming makes the final flush impossible.
                # Shutdown still has to complete, so report the loss and carry on.
                logger.warning(
                    "Could not flush the final %d events: a sink is not consuming.",
                    sum(len(buffer) for buffer in self._event_buffers),
                )

        # Drain the queue to empty. A fixed deadline here silently discarded the
        # backlog of any sink slower than the simulation, so the wait ends only when
        # the queue stops shrinking.
        if hasattr(self, "egress_queues"):
            stall_limit = getattr(self, "egress_drain_stall_seconds", 30.0)

            def total_queued() -> int:
                return sum(q.qsize() for q in self.egress_queues)

            remaining = total_queued()
            last_progress = time.time()
            last_report = time.time()

            # Every queue has to drain, so progress is measured on the total. One slow
            # sink no longer ends the wait for the others.
            while total_queued() > 0:
                time.sleep(0.1)
                current = total_queued()
                if current < remaining:
                    remaining = current
                    last_progress = time.time()
                elif time.time() - last_progress > stall_limit:
                    break
                if time.time() - last_report > 5.0:
                    last_report = time.time()
                    logger.info("Draining egress queues, %d batches left.", current)

            if total_queued() > 0:
                logger.warning(
                    "Egress queues stopped draining with %d batches left, so some final "
                    "records are lost. A sink is not keeping up: %s",
                    total_queued(),
                    ", ".join(
                        f"{type(p).__name__}={q.qsize()}"
                        for p, q in zip(self.egress_providers, self.egress_queues)
                        if q.qsize()
                    ),
                )
            else:
                # Give the final pyarrow operation a half-second to safely close the file
                time.sleep(0.5)

        # Properly wait for any executing I/O threads to finish their awaited tasks
        # by checking if any provider has active tasks in progress
        stall_limit = getattr(self, "egress_drain_stall_seconds", 30.0)
        outstanding = None
        last_progress = time.time()
        while True:
            active = sum(
                getattr(provider, "active_tasks", 0)
                for provider in getattr(self, "egress_providers", [])
            )
            if active == 0:
                break
            if outstanding is None or active < outstanding:
                outstanding = active
                last_progress = time.time()
            elif time.time() - last_progress > stall_limit:
                logger.warning(
                    "Egress providers stopped finishing tasks, %d still active.", active
                )
                break
            time.sleep(0.1)

        loop = getattr(self, "_egress_loop", None)
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)


class DynamicRealtimeEnvironment(
    RealtimeEnvironment, RegistryMixIn, IngressMixIn, EgressMixIn
):
    """
    The core simulation engine for `dynamic-des`.

    This environment extends SimPy's `RealtimeEnvironment` by incorporating
    a centralized `SimulationRegistry` for dynamic state updates, and MixIns
    for managing high-throughput asynchronous I/O with external systems.

    Attributes:
        start_datetime (datetime): The real-world clock time when the simulation started.
    """

    def __init__(
        self,
        initial_time=0,
        factor=1.0,
        strict=False,
        logical_start_time: Optional[datetime] = None,
        go_live_at: Optional[datetime] = None,
    ):
        """
        Initializes the real-time simulation environment.

        Args:
            initial_time (float, optional): The initial simulation time. Defaults to 0.
            factor (float, optional): The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.
            strict (bool, optional): If True, raises RuntimeError if simulation falls too far behind real time. Defaults to False.
            logical_start_time (datetime, optional): Overrides the environment's base clock.
                Crucial for historical backfilling (e.g., generating data from last week).
            go_live_at (datetime, optional): Logical instant at which pacing switches to
                real time. Until then the run uses `factor`, which is usually 0.0 so a
                backdated history is produced as fast as the machine allows. From that
                instant one simulated second takes one real second, so a backfill and a
                live tail run in one process instead of two.

        `go_live_at` is read against the same clock as `logical_start_time`, so both must
        be naive or both timezone-aware. An instant at or before the start of the run
        makes the whole run real time, which is the degenerate case of the same rule.
        """
        # Inject the custom time, or default to the exact moment the script executes
        self.start_datetime = logical_start_time or datetime.now()

        super().__init__(initial_time=initial_time, factor=factor, strict=strict)

        self._go_live_sim_time: Optional[float] = None
        if go_live_at is not None:
            self._go_live_sim_time = self._resolve_go_live_time(
                go_live_at, float(initial_time)
            )

        self.setup_registry()

    def _resolve_go_live_time(self, go_live_at: datetime, initial_time: float) -> float:
        """Converts the go-live instant into a simulation time, on the logical clock.

        Simulation seconds are counted from `start_datetime`, the same mapping that
        stamps every published record, so the switch lands on the timestamps the sink
        receives rather than on however long the run has been executing.
        """
        if (go_live_at.tzinfo is None) != (self.start_datetime.tzinfo is None):
            raise ValueError(
                "go_live_at and the simulation start time must both be naive or both "
                "be timezone-aware, otherwise the interval between them is undefined."
            )

        offset = (go_live_at - self.start_datetime).total_seconds()
        if offset < initial_time:
            logger.warning(
                "go_live_at (%s) is before the start of the run (%s), so the whole run "
                "is paced in real time.",
                go_live_at,
                self.start_datetime,
            )
            return initial_time
        return offset

    def _start_live_pacing(self, go_live_sim_time: float) -> None:
        """Switches pacing to real time, anchored at the go-live instant.

        `env_start` and `real_start` map simulation seconds onto the wall clock, and
        simpy sets them once when the environment is built. Leaving them there would
        make the first paced event wait out the whole backfill in real seconds, so the
        anchor moves to the go-live instant and to now.
        """
        self.env_start = go_live_sim_time
        self.real_start = time.monotonic()
        # `factor` is a read-only property in simpy, so its backing attribute is the
        # only way to change pacing during a run. `step` reads it on every call, so the
        # new value applies from the next event onwards.
        self._factor = LIVE_FACTOR
        self._go_live_sim_time = None

        # setup_egress skips the periodic flush at factor=0, because a timer in
        # simulation seconds fires constantly when the clock is detached and
        # batch_size never governs. Now that the clock is paced again the timer is
        # meaningful, and without it a live tail only reaches its sink at teardown
        # rather than as events happen, which is the whole point of going live.
        # A run that started at a non-zero factor already has its timers, and starting
        # a second set would flush each buffer twice per interval.
        if hasattr(self, "egress_queues") and not self._periodic_flush_running:
            for index in range(len(self.egress_queues)):
                self.process(self._periodic_flush(index))  # type: ignore[attr-defined]
            self._periodic_flush_running = True

        logger.info(
            "Logical clock reached go-live at %s; pacing switched to real time.",
            self._get_iso_timestamp(self.start_datetime, go_live_sim_time),
        )

    def step(self) -> None:
        """Applies the pending go-live switch before simpy paces the next event.

        The check sits here rather than in a scheduled process so that `go_live_at`
        adds no event of its own. A run that empties its schedule before the go-live
        instant still ends there instead of being held open by the switch.
        """
        go_live_sim_time = self._go_live_sim_time
        if go_live_sim_time is not None:
            next_event_time = self.peek()
            if not math.isinf(next_event_time) and next_event_time >= go_live_sim_time:
                self._start_live_pacing(go_live_sim_time)
        super().step()

    def teardown(self):
        """
        Gracefully terminates the environment.

        Ensures that any remaining data in event buffers is flushed to the
        egress connectors, and that background asyncio threads for both
        ingress and egress are cleanly stopped. Should be called in a `finally` block.
        """
        logger.info("Environment teardown initiated.")
        if hasattr(self, "teardown_egress"):
            self.teardown_egress()
        if hasattr(self, "teardown_ingress"):
            self.teardown_ingress()
        logger.info("Environment teardown complete.")

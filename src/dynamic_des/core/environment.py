import asyncio
import logging
import queue
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from simpy import RealtimeEnvironment

from dynamic_des.core.registry import SimulationRegistry
from dynamic_des.models.schemas import EventPayload, TelemetryPayload

logger = logging.getLogger(__name__)


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
        if self._ingress_loop and self._ingress_loop.is_running():
            self._ingress_loop.call_soon_threadsafe(self._ingress_loop.stop)


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
    ):
        """
        Initializes the egress buffers and starts the background publisher threads.

        Args:
            providers (List[BaseEgress]): A list of initialized egress connector instances.
            batch_size (int, optional): The maximum number of events to buffer before flushing. Defaults to 500.
            flush_interval (float, optional): Maximum simulation seconds to wait before forcing a flush. Defaults to 1.0.
        """
        self.egress_queue: queue.Queue[Any] = queue.Queue()
        self.egress_providers = providers
        self.egress_batch_size = batch_size
        self.egress_flush_interval = flush_interval
        self._event_buffer: List[Dict[str, Any]] = []
        self._egress_loop: Optional[asyncio.AbstractEventLoop] = (
            None  # Store loop reference
        )

        # Start background threads for providers
        self._egress_thread = threading.Thread(
            target=self._run_egress_loop, daemon=True
        )
        self._egress_thread.start()

        # Start background processes
        self.process(self._periodic_flush())  # type: ignore[attr-defined]
        self.process(self._lag_monitor())  # type: ignore[attr-defined]

    def _lag_monitor(self):
        """Internal: Monitors how far the simulation time has drifted from the real-world clock."""
        while True:
            # Calculate real seconds elapsed since simulation start
            real_elapsed = (datetime.now() - self.start_datetime).total_seconds()  # type: ignore[attr-defined]
            lag = max(0.0, real_elapsed - self.now)  # type: ignore[attr-defined]

            # Publish this system health metric automatically
            self.publish_telemetry("system.simulation.lag_seconds", round(lag, 3))
            yield self.timeout(1.0)  # type: ignore[attr-defined]

    def _run_egress_loop(self):
        """Internal: Runs the asyncio event loop for egress providers in a background thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._egress_loop = loop

        # Keep a list of the running tasks
        tasks = [
            loop.create_task(provider.run(self.egress_queue))
            for provider in self.egress_providers
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

    def _periodic_flush(self):
        """Internal: SimPy process that flushes the buffer at regular intervals."""
        while True:
            yield self.timeout(self.egress_flush_interval)  # type: ignore[attr-defined]
            self._flush_buffer()

    def _flush_buffer(self):
        """Internal: Pushes buffered data to the thread-safe queue."""
        if self._event_buffer:
            logger.debug(f"Flushing {len(self._event_buffer)} events to egress.")
            self.egress_queue.put(self._event_buffer)
            self._event_buffer = []

    def publish_telemetry(self, path_id: str, value: Any):
        """
        Publishes a low-volume telemetry metric (e.g., utilization, queue length).

        Telemetry triggers an immediate push to the egress queue to keep system
        vitals fresh for external dashboards.

        Args:
            path_id (str): The dot-notation path of the metric (e.g., 'Line_A.lathe.utilization').
            value (Any): The current value of the metric.
        """
        if not hasattr(self, "egress_queue"):
            return  # Fail silently if no egress is configured

        payload = TelemetryPayload(
            path_id=path_id,
            value=value,
            sim_ts=round(self.now, 3),  # type: ignore[attr-defined]
            timestamp=self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
        )

        self.egress_queue.put([payload.model_dump(mode="json")])

    def publish_event(self, event_key: str, value: Any):
        """
        Buffers a high-volume discrete event (e.g., a task starting or finishing).

        Events are buffered and flushed either when `batch_size` is reached
        or when `flush_interval` occurs to optimize external I/O throughput.

        Args:
            event_key (str): A unique identifier for the event (e.g., 'task-001').
            value (Any): A dictionary containing the event payload.
        """
        if not hasattr(self, "_event_buffer"):
            return  # Fail silently if no egress is configured

        payload = EventPayload(
            key=event_key,
            value=value,
            sim_ts=round(self.now, 3),  # type: ignore[attr-defined]
            timestamp=self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
        )

        self._event_buffer.append(payload.model_dump(mode="json"))

        if len(self._event_buffer) >= self.egress_batch_size:
            self._flush_buffer()

    def _get_iso_timestamp(self, start_time: datetime, sim_now: float) -> str:
        """Internal: Converts simulation time to a real-world ISO string."""
        return (start_time + timedelta(seconds=sim_now)).isoformat(
            timespec="milliseconds"
        )

    def teardown_egress(self):
        """Flushes final buffer contents and safely stops the egress event loop."""
        logger.info("Tearing down egress connectors, flushing final events...")
        self._flush_buffer()
        if self._egress_loop and self._egress_loop.is_running():
            self._egress_loop.call_soon_threadsafe(self._egress_loop.stop)


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

    def __init__(self, initial_time=0, factor=1.0, strict=False):
        """
        Initializes the real-time simulation environment.

        Args:
            initial_time (float, optional): The initial simulation time. Defaults to 0.
            factor (float, optional): The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.
            strict (bool, optional): If True, raises RuntimeError if simulation falls too far behind real time. Defaults to False.
        """
        self.start_datetime = datetime.now()
        super().__init__(initial_time=initial_time, factor=factor, strict=strict)
        self.setup_registry()

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

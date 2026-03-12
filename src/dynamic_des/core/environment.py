import asyncio
import logging
import queue
import threading
from datetime import datetime, timedelta
from typing import Any, List

from simpy import RealtimeEnvironment

from dynamic_des.core.registry import SimulationRegistry

logger = logging.getLogger(__name__)


class RegistryMixIn:
    def setup_registry(self):
        self.registry = SimulationRegistry(self)


class IngressMixIn:
    """Handles background I/O for Ingress (Incoming updates)."""

    def setup_ingress(self, providers: List):
        self.ingress_queue = queue.Queue()
        self.ingress_providers = providers
        self._ingress_loop = None  # Store loop reference
        self._ingress_thread = threading.Thread(
            target=self._run_ingress_loop, daemon=True
        )
        self._ingress_thread.start()
        self.process(self._ingress_monitor())

    def _run_ingress_loop(self):
        self._ingress_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._ingress_loop)
        for provider in self.ingress_providers:
            self._ingress_loop.create_task(provider.run(self.ingress_queue))
        self._ingress_loop.run_forever()

    def _ingress_monitor(self):
        while True:
            # FIX: Thread-safe queue polling to prevent race conditions
            while True:
                try:
                    path, value = self.ingress_queue.get_nowait()
                    logger.debug(f"Ingress update received: {path} = {value}")
                    self.registry.update(path, value)
                except queue.Empty:
                    break  # Queue is empty, exit the inner loop
            yield self.timeout(0.1)

    def teardown_ingress(self):
        """Safely stops the ingress event loop."""
        logger.info("Tearing down ingress connectors...")
        if self._ingress_loop and self._ingress_loop.is_running():
            self._ingress_loop.call_soon_threadsafe(self._ingress_loop.stop)


class EgressMixIn:
    """MixIn to handle high-throughput egress with count or time-based flushing."""

    def setup_egress(
        self, providers: List, batch_size: int = 500, flush_interval: float = 1.0
    ):
        self.egress_queue = queue.Queue()
        self.egress_providers = providers
        self.egress_batch_size = batch_size
        self.egress_flush_interval = flush_interval
        self._event_buffer = []
        self._egress_loop = None  # Store loop reference

        # Start background threads for providers
        self._egress_thread = threading.Thread(
            target=self._run_egress_loop, daemon=True
        )
        self._egress_thread.start()

        # Start periodic flush process (Time-based trigger)
        self.process(self._periodic_flush())

    def _run_egress_loop(self):
        """Background thread logic for egress providers."""
        self._egress_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._egress_loop)
        for provider in self.egress_providers:
            self._egress_loop.create_task(provider.run(self.egress_queue))
        self._egress_loop.run_forever()

    def _periodic_flush(self):
        """SimPy process that flushes the buffer every x seconds."""
        while True:
            yield self.timeout(self.egress_flush_interval)
            self._flush_buffer()

    def _flush_buffer(self):
        """Internal helper to push data to the thread-safe queue."""
        if self._event_buffer:
            logger.debug(f"Flushing {len(self._event_buffer)} events to egress.")
            self.egress_queue.put(self._event_buffer)
            self._event_buffer = []

    def publish_telemetry(self, path_id: str, value: Any):
        # ... (Method remains unchanged) ...
        self.egress_queue.put(
            [
                {
                    "stream_type": "telemetry",
                    "path_id": path_id,
                    "value": value,
                    "sim_ts": round(self.now, 3),
                    "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),
                }
            ]
        )

    def publish_event(self, event_key: str, value: Any):
        # ... (Method remains unchanged) ...
        self._event_buffer.append(
            {
                "stream_type": "event",
                "key": event_key,
                "value": value,
                "sim_ts": round(self.now, 3),
                "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),
            }
        )

        if len(self._event_buffer) >= self.egress_batch_size:
            self._flush_buffer()

    def _get_iso_timestamp(self, start_time: datetime, sim_now: float) -> str:
        """Converts simulation time to a real-world ISO string."""
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
    def __init__(self, initial_time=0, factor=1.0, strict=False):
        self.start_datetime = datetime.now()
        super().__init__(initial_time=initial_time, factor=factor, strict=strict)
        self.setup_registry()

    def teardown(self):
        """
        Gracefully terminates the environment, ensuring buffers are flushed
        and background threads are sent stop signals.
        """
        logger.info("Environment teardown initiated.")
        if hasattr(self, "teardown_egress"):
            self.teardown_egress()
        if hasattr(self, "teardown_ingress"):
            self.teardown_ingress()
        logger.info("Environment teardown complete.")

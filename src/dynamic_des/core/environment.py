import asyncio
import queue
import threading
from datetime import datetime, timedelta
from typing import Any, List

from simpy import RealtimeEnvironment

from dynamic_des.core.registry import SimulationRegistry


class RegistryMixIn:
    def setup_registry(self):
        self.registry = SimulationRegistry(self)


class IngressMixIn:
    """Handles background I/O for Ingress (Incoming updates)."""

    def setup_ingress(self, providers: List):
        self.ingress_queue = queue.Queue()
        self.ingress_providers = providers
        self._ingress_thread = threading.Thread(
            target=self._run_ingress_loop, daemon=True
        )
        self._ingress_thread.start()
        self.process(self._ingress_monitor())

    def _run_ingress_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for provider in self.ingress_providers:
            loop.create_task(provider.run(self.ingress_queue))
        loop.run_forever()

    def _ingress_monitor(self):
        while True:
            while not self.ingress_queue.empty():
                path, value = self.ingress_queue.get_nowait()
                self.registry.update(path, value)
            yield self.timeout(0.1)


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

        # Start background threads for providers
        self._egress_thread = threading.Thread(
            target=self._run_egress_loop, daemon=True
        )
        self._egress_thread.start()

        # Start periodic flush process (Time-based trigger)
        self.process(self._periodic_flush())

    def _run_egress_loop(self):
        """Background thread logic for egress providers."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for provider in self.egress_providers:
            # Each provider's run method takes the egress_queue as an argument
            loop.create_task(provider.run(self.egress_queue))
        loop.run_forever()

    def _periodic_flush(self):
        """SimPy process that flushes the buffer every x seconds."""
        while True:
            yield self.timeout(self.egress_flush_interval)
            self._flush_buffer()

    def _flush_buffer(self):
        """Internal helper to push data to the thread-safe queue."""
        if self._event_buffer:
            self.egress_queue.put(self._event_buffer)
            self._event_buffer = []

    def publish_telemetry(self, path_id: str, value: Any):
        """Telemetry usually triggers an immediate flush to keep vitals fresh."""
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
        """Event data: Flushed if batch size is reached OR periodic flush occurs."""
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


class DynamicRealtimeEnvironment(
    RealtimeEnvironment, RegistryMixIn, IngressMixIn, EgressMixIn
):
    def __init__(self, initial_time=0, factor=1.0, strict=False):
        self.start_datetime = datetime.now()
        super().__init__(initial_time=initial_time, factor=factor, strict=strict)
        self.setup_registry()

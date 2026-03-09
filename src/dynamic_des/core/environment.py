import asyncio
import queue
import threading
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
    """MixIn to handle high-throughput outgoing telemetry and events."""

    def setup_egress(self, providers: List, batch_size: int = 500):
        self.egress_queue = queue.Queue()
        self.egress_providers = providers
        self.egress_batch_size = batch_size
        self._event_buffer = []

        # Start the background thread for Egress providers
        self._egress_thread = threading.Thread(
            target=self._run_egress_loop, daemon=True
        )
        self._egress_thread.start()

    def _run_egress_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for provider in self.egress_providers:
            loop.create_task(provider.run(self.egress_queue))
        loop.run_forever()

    def publish_telemetry(self, path_id: str, value: Any):
        """Low-volume vitals. Pushed to queue immediately."""
        self.egress_queue.put(
            [
                {
                    "stream_type": "telemetry",
                    "path_id": path_id,
                    "value": value,
                    "timestamp": self.now,
                }
            ]
        )

    def publish_event(self, event_key: str, value: Any):
        """High-volume results. Batched locally to reduce queue lock contention."""
        self._event_buffer.append(
            {
                "stream_type": "event",
                "key": event_key,
                "value": value,
                "timestamp": self.now,
            }
        )

        if len(self._event_buffer) >= self.egress_batch_size:
            # Push the whole batch to the thread-safe queue
            self.egress_queue.put(self._event_buffer)
            self._event_buffer = []


class DynamicRealtimeEnvironment(
    RealtimeEnvironment, RegistryMixIn, IngressMixIn, EgressMixIn
):
    def __init__(self, initial_time=0, factor=1.0, strict=False):
        super().__init__(initial_time=initial_time, factor=factor, strict=strict)
        self.setup_registry()

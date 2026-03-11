import json
from collections import defaultdict
from typing import Any, Dict, List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


class KafkaAdminConnector:
    """
    Unified Kafka Admin and Monitoring Connector.
    Uses aiokafka for async data flow and kafka-python for sync admin tasks.
    """

    def __init__(self, bootstrap_servers: str, max_tasks: int = 100, **kwargs):
        """
        Initialize the connector.

        Args:
            bootstrap_servers: Kafka broker addresses.
            max_tasks: The maximum number of task records to keep per service
                in memory (rolling window).
            **kwargs: Additional arguments passed to Kafka clients.
        """
        self.bootstrap_servers = bootstrap_servers
        self.max_tasks = max_tasks
        self.kwargs = kwargs

        # Event State: sim_id -> service -> task_id -> {status: timestamp}
        self._state = defaultdict(lambda: defaultdict(dict))
        # Telemetry State: path_id -> latest_value
        self._vitals: Dict[str, Any] = {}

    def create_topics(self, topics_config: List[Dict[str, Any]]):
        """
        Creates Kafka topics. Synchronous call to ensure infrastructure
        is ready before simulation start.
        """
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="sim_admin",
            **self.kwargs,
        )

        new_topics = [
            NewTopic(
                name=cfg["name"],
                num_partitions=cfg.get("partitions", 1),
                replication_factor=cfg.get("replication", 1),
            )
            for cfg in topics_config
        ]

        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
        except TopicAlreadyExistsError:
            pass
        finally:
            admin_client.close()

    async def send_config(self, topic: str, path_id: str, value: Any):
        """
        Sends a surgical parameter update to the simulation config topic.
        """
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers, **self.kwargs
        )
        await producer.start()
        try:
            payload = json.dumps({"path_id": path_id, "value": value}).encode("utf-8")
            await producer.send_and_wait(topic, payload)
        finally:
            await producer.stop()

    async def collect_data(self, topics: List[str], auto_offset_reset="latest"):
        """Async loop to consume from telemetry and event topics."""
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            **self.kwargs,
        )
        await consumer.start()
        try:
            async for msg in consumer:
                data = json.loads(msg.value.decode("utf-8"))
                self._process_message(data)
        finally:
            await consumer.stop()

    def _process_message(self, data: Dict[str, Any]):
        """Routes message based on JSON structure."""

        # 1. Telemetry: 'path_id' is at the root level
        if "path_id" in data and not isinstance(data.get("value"), dict):
            self._vitals[data["path_id"]] = data["value"]

        # 2. Events: 'key' is at the root, 'value' is a dictionary
        elif "key" in data and isinstance(data.get("value"), dict):
            task_id = data["key"]
            payload = data["value"]
            status = payload.get("status")

            # Using 'timestamp' based on your actual JSON payload
            ts = data.get("timestamp")

            path_id = payload.get("path_id", "unknown.unknown.unknown")
            parts = path_id.split(".")
            sim_id = parts[0]
            service = parts[2] if len(parts) > 2 else "default"

            service_data = self._state[sim_id][service]

            # Prune oldest if at max capacity
            if task_id not in service_data:
                if len(service_data) >= self.max_tasks:
                    oldest_key = next(iter(service_data))
                    service_data.pop(oldest_key)

            # Update task status timestamp
            if task_id not in service_data:
                service_data[task_id] = {}

            service_data[task_id][status] = ts

    def get_vitals(self) -> Dict[str, Any]:
        """Returns the latest system telemetry (Capacity, Utilization, etc)."""
        return self._vitals

    def get_state(self) -> Dict[str, Any]:
        """Returns the aggregated event state (Task lifecycles)."""
        return self._state

# Complex Routing (Kafka)

In dynamic simulations, not all events are created equal. You may want standard event logs to go to one Kafka topic, system metrics to another, and critical error alarms to a high-priority alarm queue or dead-letter topic.

Dynamic DES supports this via **Topic Routing (Multiplexing)**. Instead of hardcoding a single event topic, you can pass a dynamic `topic_router` function into your egress connectors.

The snippets below publish to Kafka, so start a broker first with `odctl up kafka-lite`. See [Getting Started](../getting-started.md) for the one-time odctl install. Tear it down with `odctl down kafka-lite --volumes`.

---

## Writing a Custom Router

A topic router is a standard Python function that receives a dictionary representing the serialized event payload, and returns the target topic name as a string. `KafkaEgress` sends every record it receives, so the router must always return a topic. To drop records, give the provider a `when` predicate on `add_egress`, which filters them before they reach the router.

```text
               ┌───────────────────────┐
               │    Egress Connector   │
               └───────────┬───────────┘
                           │ Data Dictionary
                           v
               ┌───────────────────────┐
               │ custom_kafka_router(d)│
               └─┬─────────┬─────────┬─┘
                 │         │         │
"factory-events" v         │         v  "system-alarms"
  ┌──────────────┐         │         ┌──────────────┐
  │ Kafka Topic  │         │         │ Kafka Topic  │
  └──────────────┘         v         └──────────────┘
                  "factory-telemetry"
                    ┌──────────────┐
                    │ Kafka Topic  │
                    └──────────────┘
```

Here is a concrete example routing different events based on payload metadata:

```python
from typing import Dict, Any

def custom_kafka_router(data: Dict[str, Any]) -> str:
    """
    Routes messages based on event importance and metadata.
    """
    stream_type = data.get("stream_type")

    # 1. Route continuous telemetry to metrics topic
    if stream_type == "telemetry":
        return "factory-telemetry"

    # 2. Filter out normal task events vs critical errors
    if stream_type == "event":
        value = data.get("value", {})

        # Route machine failures to high-priority alarms topic
        if value.get("status") == "machine_failed" or value.get("level") == "ERROR":
            return "system-alarms"

    # Route standard lifecycle logs, and anything unrecognized, to the events topic
    return "factory-events"
```

---

## Hooking up the Router to Kafka Egress

Pass your custom router function as the `topic_router` parameter to `KafkaEgress`.

```python
from dynamic_des import SimulationContext, KafkaEgress

app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_resource("lathe", current_cap=1, max_cap=1)
    .add_egress(KafkaEgress(
        bootstrap_servers="localhost:9092",
        topic_router=custom_kafka_router
    ))
)
```

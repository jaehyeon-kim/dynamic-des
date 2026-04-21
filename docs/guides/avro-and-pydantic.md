# Advanced Serialization: Avro & Pydantic

By default, `KafkaEgress` strictly dumps all payloads as highly optimized JSON. While great for lightweight lifecycle and telemetry events, high-velocity ML streams (like `PredictionRequests`) often require compact, schema-validated binary payloads using Avro.

Dynamic DES provides a **Pluggable Serialization Strategy**, allowing you to seamlessly mix JSON and Avro streams on the exact same connection, while natively supporting Pydantic validation.

## Pydantic Duck-Typing

You do not need to convert your data to dictionaries manually. `KafkaEgress` uses duck-typing to automatically detect and extract data from Pydantic V1 and V2 models.

You can yield raw, strongly-typed models directly from your simulation logic:

```python
from pydantic import BaseModel
from dynamic_des import DynamicRealtimeEnvironment

class TaskEvent(BaseModel):
    path_id: str
    status: str

def work_task(env: DynamicRealtimeEnvironment, task_id: int):
    # Pass the Pydantic model directly to the framework!
    event = TaskEvent(path_id="Line_A.lathe", status="queued")
    env.publish_event(f"task-{task_id}", event)
```

## Pluggable Topic Routing

Now that you are yielding Pydantic models, you need a way to tell Dynamic DES which models should remain standard JSON, and which ones need to be serialized as Avro. We do this using Topic Routing.

You can configure `KafkaEgress` to route different events to different topics, and apply specific serializers (like Confluent or AWS Glue Schema Registries) only where needed. Any topic that does not have an explicit serializer mapped will safely fall back to the default `JsonSerializer`.

```python
from dynamic_des import KafkaEgress
from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer

# 1. Initialize your Avro Serializers for specific ML schemas
prediction_serializer = ConfluentAvroSerializer(
    registry_url="http://localhost:8081",
    schema_str="""{"type": "record", "name": "Prediction", "fields": [...]}"""
)

# 2. Define a topic router to split standard events from ML events
def ml_topic_router(data: dict) -> str:
    stream_type = data.get("stream_type")

    if stream_type == "telemetry":
        return "sim-telemetry"

    value = data.get("value", {})
    if value.get("event_type") == "prediction":
        return "ml-predictions"

    return "sim-events"

# 3. Configure the Egress Connector
egress = KafkaEgress(
    bootstrap_servers="localhost:9092",
    topic_router=ml_topic_router,

    # Map the ML topic to Avro.
    # 'sim-telemetry' and 'sim-events' will automatically fall back to JSON!
    topic_serializers={
        "ml-predictions": prediction_serializer,
    }
)
```

## Auto-Generating Avro Schemas

Hardcoding Avro JSON strings in Python is prone to error and schema drift. The industry best practice is to generate your Avro schema _directly_ from your Pydantic model.

Dynamic DES officially recommends using the `dataclasses-avroschema` library for this.

### Define your model using AvroBaseModel

```python
from pydantic import Field
from dataclasses_avroschema.pydantic import AvroBaseModel

class MLPrediction(AvroBaseModel):
    """My high-velocity ML payload"""
    task_id: str = Field(...)
    confidence: float = Field(...)

    class Meta:
        namespace = "com.dynamic_des.ml"
        schema_name = "PredictionEvent"

# Automatically generate the Avro schema string!
schema_string = MLPrediction.avro_schema()
```

### Plug it into the Egress Connector

Both `ConfluentAvroSerializer` and `GlueAvroSerializer` accept the `schema_str` argument. 

Once you have initialized the serializer with your auto-generated schema, the final step is to pass it into your `KafkaEgress` topic configuration.

```python
from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer
from dynamic_des import KafkaEgress

# 1. Initialize with the auto-generated schema
confluent_serializer = ConfluentAvroSerializer(
    registry_url="http://localhost:8081",
    schema_str=MLPrediction.avro_schema() # Always up to date!
)

# 2. Map it to your topic router!
egress = KafkaEgress(
    bootstrap_servers="localhost:9092",
    topic_router=ml_topic_router,
    topic_serializers={
        "ml-predictions": confluent_serializer, # Specify the serializer for a topic!
    }
)
```
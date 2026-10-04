# Kafka Digital Twin

A production line that takes parameter updates from a Kafka topic and publishes its events and telemetry to Kafka topics. Each tab shows the example written one way: with the declarative API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

=== "Declarative"

    This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the declarative **Standard API (`SimulationContext`)**.

    By replacing the local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

    **Quick Start**

    Download the script, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/kafka_example.py
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the declarative simulation (Ctrl + C to stop)
    uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

    # 4. In a second terminal, watch and steer the run from the dashboard. It serves
    #    http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
    uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py

    # 5. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the kafka extra, odctl for the containers and
    #    nicegui for the dashboard
    pip install "dynamic-des[kafka]" "odctl>=0.5.1" nicegui

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the declarative simulation (Ctrl + C to stop)
    python kafka_example.py

    # 4. In a second terminal, watch and steer the run from the dashboard. It serves
    #    http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
    python kafka_dashboard.py

    # 5. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    The run keeps going until you stop it. It logs one line per task as the task claims the lathe, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

    **Full Source Code**

    This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/kafka_example.py"
    """Kafka Digital Twin, declarative API.

    Builds `Line_A` with `SimulationContext` and connects it to Kafka in both directions.
    `KafkaIngress` reads parameter updates from `sim-config`, so the running simulation can
    be steered without restarting it. `KafkaEgress` publishes lifecycle events to
    `sim-events` and metrics to `sim-telemetry`, with Pydantic models giving the events a
    declared shape.

    Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
    """

    import logging
    import os
    import time

    from pydantic import BaseModel

    from dynamic_des import (
        KafkaAdminConnector,
        KafkaEgress,
        KafkaIngress,
        SimulationContext,
    )

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
    )
    logger = logging.getLogger("kafka_example")


    # ==========================================
    # 1. Define Strongly-Typed Event Payloads
    # ==========================================
    class TaskEvent(BaseModel):
        """
        Strongly typed event payload to guarantee schema consistency
        when shipping data over the wire to Kafka.
        """

        path_id: str
        status: str


    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # ==========================================
    # 2. Declarative Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(sim_id="Line_A", factor=1.0, random_seed=42)
        .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
        .add_egress(
            KafkaEgress(
                event_topic="sim-events",
                telemetry_topic="sim-telemetry",
                bootstrap_servers=BOOTSTRAP_SERVERS,
            )
        )
        .add_resource("lathe", current_cap=1, max_cap=10)
        .add_service("milling", dist="normal", mean=3.0, std=0.5)
        .add_arrival("standard", dist="exponential", rate=1.0)
    )


    # ==========================================
    # 3. Simulation Logic
    # ==========================================
    @app.task(service_id="milling", resource_id="lathe")
    def process_part(task_id: int, context):
        """
        The @task decorator automatically locks the resource and emits the
        'queued' and 'started' events. We just execute our custom logic
        and return the final payload.
        """
        logger.info(f"Task {task_id} started at sim time: {context._env.now:.2f}s")

        # Return the strongly-typed Pydantic model for the 'finished' state
        return TaskEvent(path_id="Line_A.service.milling", status="finished").model_dump(
            mode="json"
        )


    @app.arrival_loop("standard")
    def arrival_generator(context):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(process_part(task_id, context))
            task_id += 1


    @app.telemetry_loop(interval=2.0)
    def telemetry_monitor(context):
        """Low-volume system health stream."""
        res = context.get_resource("lathe")

        # Exact parity with the imperative telemetry outputs
        context.publish("lathe.capacity", res.capacity)
        context.publish("lathe.in_use", res.in_use)
        context.publish("lathe.queue_length", len(res.queue.items))

        util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
        context.publish("lathe.utilization", util)

        avg_wait = len(res.queue.items) * 3.0
        context.publish("lathe.avg_wait", avg_wait)


    # ==========================================
    # 4. Execution
    # ==========================================
    def run():
        TOPICS_CONFIG = [
            {"name": "sim-config", "partitions": 1},
            {"name": "sim-telemetry", "partitions": 1},
            {"name": "sim-events", "partitions": 1},
        ]

        logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
        try:
            admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
            admin.create_topics(topics_config=TOPICS_CONFIG)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Could not explicitly create topics: {e}")

        print("Simulation started.")
        print("  - Listen to 'sim-telemetry' for system vitals.")
        print("  - Listen to 'sim-events' for task lifecycles.")
        print("  - Send to 'sim-config' to update parameters.")

        app.run()  # Starts the clock and orchestrates all connectors


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the low-level **Imperative API**.

    By replacing the Local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

    **Quick Start**

    Download the script, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/kafka_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the imperative simulation (Ctrl + C to stop)
    uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the kafka extra, and odctl for the containers
    pip install "dynamic-des[kafka]" "odctl>=0.5.1"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the imperative simulation (Ctrl + C to stop)
    python kafka_example.py

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    The run keeps going until you stop it. It logs one line per task as the task claims the lathe, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

    **Full Source Code**

    This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/imperative/kafka_example.py"
    """Kafka Digital Twin, imperative API.

    The low-level twin of `declarative/kafka_example.py`, wiring the environment, registry
    and connectors by hand. Like the declarative version, it creates its topics first with
    `KafkaAdminConnector`.

    Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
    """

    import logging
    import os
    import time

    import numpy as np
    from pydantic import BaseModel

    from dynamic_des import (
        CapacityConfig,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        KafkaAdminConnector,
        KafkaEgress,
        KafkaIngress,
        Sampler,
        SimParameter,
    )

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
    )
    logger = logging.getLogger("kafka_example")


    # ==========================================
    # 1. Define Strongly-Typed Event Payloads
    # ==========================================
    class TaskEvent(BaseModel):
        """
        Thanks to dynamic-des's duck-typing, we can pass this Pydantic model
        directly into env.publish_event(). The KafkaEgress layer will seamlessly
        extract it and serialize it (either to JSON or Avro).
        """

        path_id: str
        status: str


    def run():
        # 2. Create Kafka topics
        BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        TOPICS_CONFIG = [
            {"name": "sim-config", "partitions": 1},
            {"name": "sim-telemetry", "partitions": 1},
            {"name": "sim-events", "partitions": 1},
        ]

        logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
        admin_connector = KafkaAdminConnector(
            bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
        )
        admin_connector.create_topics(topics_config=TOPICS_CONFIG)
        time.sleep(2)

        # 3. Define initial system state
        line_a_params = SimParameter(
            sim_id="Line_A",
            arrival={
                "standard": DistributionConfig(dist="exponential", rate=1.0)
            },  # 1 every 1s
            service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
            resources={"lathe": CapacityConfig(current_cap=1, max_cap=10)},
        )

        # 4. Setup Environment with Kafka Connectors
        # (Optional: Pass **kwargs like `security_protocol="SASL_SSL"` for enterprise clusters)
        ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

        # By default, this uses JsonSerializer. To use Avro for enterprise environments:
        # from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer
        # avro_serializer = ConfluentAvroSerializer(
        #     registry_url="http://127.0.0.1:8081", schema_str=AVRO_SCHEMA
        # )
        # Then pass: topic_serializers={"sim-events": avro_serializer}
        # Generate AVRO_SCHEMA from AvroBaseModel rather than a plain Pydantic model;
        # see the Avro and Pydantic guide. TaskEvent above has no .avro_schema().
        egress = KafkaEgress(
            telemetry_topic="sim-telemetry",
            event_topic="sim-events",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )

        env = DynamicRealtimeEnvironment(factor=1.0)
        env.registry.register_sim_parameter(line_a_params)
        env.setup_ingress([ingress])
        env.setup_egress([egress])

        # 5. Initialize Resources and Sampler
        res = DynamicResource(env, "Line_A", "lathe")
        sampler = Sampler(rng=np.random.default_rng(42))

        # 6. Define Simulation Logic
        def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
            arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
            service_path = "Line_A.service.milling"
            task_id = 0

            while True:
                yield env.timeout(sampler.sample(arrival_cfg))
                env.process(work_task(env, task_id, res, service_path))
                task_id += 1

        def work_task(
            env: DynamicRealtimeEnvironment,
            task_id: int,
            res: DynamicResource,
            path_id: str,
        ):
            task_key = f"task-{task_id}"

            # Publish Pydantic model instead of raw dictionary
            env.publish_event(task_key, TaskEvent(path_id=path_id, status="queued"))

            with res.request() as req:
                yield req

                logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
                env.publish_event(task_key, TaskEvent(path_id=path_id, status="started"))

                # Use latest service config from registry
                service_cfg = env.registry.get_config(path_id)
                yield env.timeout(sampler.sample(service_cfg))

                env.publish_event(task_key, TaskEvent(path_id=path_id, status="finished"))

        def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
            """Low-volume system health stream."""
            while True:
                # Pushed to 'sim-telemetry' topic
                env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
                env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
                env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

                util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
                env.publish_telemetry("Line_A.lathe.utilization", util)

                yield env.timeout(2.0)

        # 7. Run
        env.process(arrival_process(env, res))
        env.process(telemetry_monitor(env, res))

        print("Simulation started.")
        print("  - Listen to 'sim-telemetry' for system vitals.")
        print("  - Listen to 'sim-events' for task lifecycles.")
        print("  - Send to 'sim-config' to update parameters.")

        try:
            env.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")
        finally:
            env.teardown()


    if __name__ == "__main__":
        run()
    ```

=== "YAML"

    This example builds the simulation of the declarative example in the Declarative tab from a YAML blueprint, with no Python. The connectors, the parameters, the task and the telemetry are all declared in the file.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/kafka.yaml
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the blueprint (Ctrl + C to stop)
    uv run --no-project --with "dynamic-des[kafka]" ddes run kafka.yaml

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the kafka extra, and odctl for the containers
    pip install "dynamic-des[kafka]" "odctl>=0.5.1"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the blueprint (Ctrl + C to stop)
    ddes run kafka.yaml

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **What It Does**

    The run keeps going until you stop it. When it starts, `KafkaEgress` creates `sim-events` and `sim-telemetry` if they do not exist. The run then publishes task lifecycle events to `sim-events` and lathe metrics to `sim-telemetry`, and applies parameter updates sent to `sim-config`.

    The dashboard from the declarative example works with this run unchanged, because it reads the same topics and the same four lathe metrics: `uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py`, after downloading [`kafka_dashboard.py`](https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py).

    `KAFKA_BOOTSTRAP_SERVERS` overrides the broker address, because the blueprint reads it with `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`.

    **Full Source Code**

    The blueprint wires both Kafka connectors and declares the lathe, the service, the arrival and the task. The finished event of each task is the fixed `payload`, and the `telemetry` entry publishes four built-in statistics of the lathe every 2 simulation seconds. The declarative example also publishes `lathe.avg_wait`, a derived metric, which a blueprint computes only with Python.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/kafka.yaml"
    # Kafka digital twin in YAML.
    #
    # The YAML version of examples/declarative/kafka_example.py. KafkaIngress reads
    # parameter updates from sim-config, and KafkaEgress publishes lifecycle events to
    # sim-events and lathe metrics to sim-telemetry. KafkaEgress creates its two topics
    # when the run starts.
    #
    # Needs a broker: odctl up kafka-lite. Runs until interrupted with Ctrl + C.
    # KAFKA_BOOTSTRAP_SERVERS overrides the broker address.
    # Run it with: ddes run examples/yaml/kafka.yaml

    simulation:
      sim_id: Line_A
      factor: 1.0
      random_seed: 42

    ingress:
      - type: Kafka
        config:
          topic: sim-config
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

    egress:
      - type: Kafka
        config:
          event_topic: sim-events
          telemetry_topic: sim-telemetry
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

    resources:
      lathe: {current_cap: 1, max_cap: 10}

    services:
      milling: {dist: normal, mean: 3.0, std: 0.5}

    arrivals:
      standard: {dist: exponential, rate: 1.0, spawn: process_part}

    tasks:
      process_part:
        service: milling
        resource: lathe
        payload: {path_id: Line_A.service.milling, status: finished}

    telemetry:
      # Samples the lathe every 2 simulation seconds.
      - interval: 2.0
        publish:
          lathe.capacity: lathe.capacity
          lathe.in_use: lathe.in_use
          lathe.queue_length: lathe.queue_length
          lathe.utilization: lathe.utilization
    ```

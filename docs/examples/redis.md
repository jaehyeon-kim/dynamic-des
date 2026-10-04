# In-Memory Store (Redis)

A factory that writes part records to a Redis Stream and takes parameter updates from a Redis Pub/Sub channel. Each tab shows the example written one way: with the declarative API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

=== "Declarative"

    This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the declarative **Standard API (`SimulationContext`)**.

    By combining `RedisIngress` and `RedisEgress`, your simulation can achieve sub-millisecond latency for both reading dynamic parameters via Pub/Sub and writing high-throughput telemetry data via Redis Streams.

    **1. Streaming to Redis**

    When generating events, `RedisEgress` streams outputs directly into a Redis Stream with `XADD`. The stream is the one named in the constructor, which this example sets to `events`.

    ```python
    app = (
        SimulationContext(sim_id="Factory", factor=1.0)
        .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
    )

    # ... inside the generator
    part_event = {
        "__stream__": "part_events",
        "part_id": 1,
        "status": "arrived",
    }
    context.publish("factory_event", part_event)
    ```

    Each entry holds one field, `payload`, containing the JSON of the published record. `RedisEgress` reads `__stream__` from inside `value`, which is where `publish_event` puts the dictionary you pass it, so part records go to `part_events` and everything else to the `events` stream named in the constructor. The key itself is removed before writing, so it does not appear in the stored payload.

    **2. Dynamic Parameter Updates (Ingress)**

    This example attaches a `RedisIngress` listening to a Pub/Sub channel called `simulation_params`. While the simulation is running, you can dynamically update parameters (like speeding up the arrival rate) by simply publishing a JSON string to the channel!

    ```python
    app.add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
    ```

    **3. Quick Start**

    Download the script, then run it. The run keeps generating parts until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the `PUBLISH` command below.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/redis_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Spin up the Valkey database
    odctl up valkey

    # 3. Run the declarative simulation
    uv run --no-project --with "dynamic-des[redis]" redis_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the redis extra, and odctl for the containers
    pip install "dynamic-des[redis]" "odctl>=0.5.1"

    # 2. Spin up the Valkey database
    odctl up valkey

    # 3. Run the declarative simulation
    python redis_example.py
    ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the Valkey container and publish the parameter update
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
    ```
    `RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN part_events` climbs about five times faster than before.

    ```bash
    # Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/redis_example.py"
    """Redis Streams output with live parameter updates, declarative API.

    `Factory` writes part records to the `part_events` Redis Stream through `RedisEgress`,
    named by each record's `__stream__` key, while `RedisIngress` subscribes to the `simulation_params` channel, so publishing a message to
    that channel changes the arrival rate of a running simulation.

    Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
    """

    import logging
    import random
    from datetime import datetime

    from dynamic_des import RedisEgress, RedisIngress, SimulationContext

    logger = logging.getLogger(__name__)

    # The odctl `valkey` profile disables the unauthenticated default user, so the
    # URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
    REDIS_URL = "redis://user:password@localhost:6379/0"

    app = (
        SimulationContext(sim_id="Factory", factor=1.0)
        .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
        .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
        .add_arrival("part_arrival", dist="exponential", rate=2.0)
    )


    @app.arrival_loop("part_arrival")
    def part_generator(context):
        part_id = 1
        while True:
            yield context.wait_for_arrival("part_arrival")

            part_event = {
                "__stream__": "part_events",
                "part_id": part_id,
                "type": random.choice(["A", "B", "C"]),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "arrived",
            }

            context.publish("factory_event", part_event)
            part_id += 1


    def run():
        logger.info("Starting Declarative Redis Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
            "--pass password PUBLISH simulation_params "
            '\'{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}\''
        )
        try:
            app.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run()
    ```

=== "Low-level"

    This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the low-level **Imperative API (`DynamicRealtimeEnvironment`)**.

    This is useful if you are migrating existing SimPy generators and prefer to handle `env.process()` and component registration manually rather than using the Builder Pattern.

    **1. Quick Start**

    Download the script, then run it. The run keeps generating parts until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the `PUBLISH` command below.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/redis_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Spin up the Valkey database
    odctl up valkey

    # 3. Run the imperative simulation
    uv run --no-project --with "dynamic-des[redis]" redis_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the redis extra, and odctl for the containers
    pip install "dynamic-des[redis]" "odctl>=0.5.1"

    # 2. Spin up the Valkey database
    odctl up valkey

    # 3. Run the imperative simulation
    python redis_example.py
    ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the Valkey container and publish the parameter update
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
    ```
    `RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN part_events` climbs about five times faster than before.

    ```bash
    # Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/imperative/redis_example.py"
    """Redis Streams output with live parameter updates, imperative API.

    The low-level twin of `declarative/redis_example.py`, wiring the environment and
    connectors by hand. `RedisEgress` writes part records to the `part_events` stream,
    named by each record's `__stream__` key, and `RedisIngress` subscribes to the
    `simulation_params` channel.

    Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
    """

    import logging
    import random
    from datetime import datetime

    import numpy as np

    from dynamic_des import (
        DistributionConfig,
        DynamicRealtimeEnvironment,
        RedisEgress,
        RedisIngress,
        Sampler,
        SimParameter,
    )

    logger = logging.getLogger("redis_example")

    # The odctl `valkey` profile disables the unauthenticated default user, so the
    # URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
    REDIS_URL = "redis://user:password@localhost:6379/0"


    def run():
        params = SimParameter(
            sim_id="Factory",
            arrival={"part_arrival": DistributionConfig(dist="exponential", rate=2.0)},
        )

        # Attach egress instance
        egress = RedisEgress(REDIS_URL, stream_name="events")

        # Attach ingress for dynamic parameter updates
        ingress = RedisIngress(REDIS_URL, channel_name="simulation_params")

        env = DynamicRealtimeEnvironment(factor=1.0)
        env.registry.register_sim_parameter(params)
        env.setup_egress([egress])
        env.setup_ingress([ingress])

        sampler = Sampler(rng=np.random.default_rng(42))

        def part_process(env: DynamicRealtimeEnvironment):
            arrival_cfg = env.registry.get_config("Factory.arrival.part_arrival")
            part_id = 1

            while True:
                yield env.timeout(sampler.sample(arrival_cfg))

                part_event = {
                    "__stream__": "part_events",
                    "part_id": part_id,
                    "type": random.choice(["A", "B", "C"]),
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "arrived",
                }
                env.publish_event(f"part-{part_id}", part_event)

                part_id += 1

        env.process(part_process(env))

        logger.info("Starting Imperative Redis Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
            "--pass password PUBLISH simulation_params "
            '\'{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}\''
        )

        try:
            env.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run()
    ```

=== "YAML"

    This example builds the simulation of the declarative example in the Declarative tab from a YAML blueprint, with no Python. `RedisEgress` writes part records to the `part_events` stream, which each record's `__stream__` key names, and `RedisIngress` subscribes to the `simulation_params` channel.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/redis.yaml
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Start the Valkey database
    odctl up valkey

    # 3. Run the blueprint (Ctrl + C to stop)
    uv run --no-project --with "dynamic-des[redis]" ddes run redis.yaml

    # 4. Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the redis extra, and odctl for the containers
    pip install "dynamic-des[redis]" "odctl>=0.5.1"

    # 2. Start the Valkey database
    odctl up valkey

    # 3. Run the blueprint (Ctrl + C to stop)
    ddes run redis.yaml

    # 4. Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **What It Does**

    The run writes part events to the `part_events` stream, and the lag telemetry to `events`, until you stop it. In a second terminal, raise the arrival rate while it runs:

    ```bash
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
    ```

    `RedisIngress` does not log the message it receives, so the sign that the update landed is that `XLEN part_events` climbs about five times faster.

    **Full Source Code**

    `record_part` has no service or resource, so it publishes its fixed `payload` as soon as an arrival spawns it, with the task id added as `part_id`. The declarative example draws a random part type for every arrival. A mapping payload is a constant, so every part here has type `A`.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/redis.yaml"
    # Redis Streams output with live parameter updates, in YAML.
    #
    # The YAML version of examples/declarative/redis_example.py. Each part_arrival
    # spawns record_part, a task with no service or resource, which publishes its
    # payload at once. RedisEgress writes it to the part_events stream that the
    # __stream__ key names, while RedisIngress subscribes to the simulation_params
    # channel.
    #
    # Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
    # Run it with: ddes run examples/yaml/redis.yaml

    simulation:
      sim_id: Factory
      factor: 1.0

    ingress:
      - type: Redis
        config:
          # The odctl valkey profile disables the unauthenticated default user, so the
          # URL carries credentials.
          url: redis://user:password@localhost:6379/0
          channel_name: simulation_params

    egress:
      - type: Redis
        config:
          url: redis://user:password@localhost:6379/0
          stream_name: events

    arrivals:
      part_arrival: {dist: exponential, rate: 2.0, spawn: record_part}

    tasks:
      record_part:
        # id_field adds the task id as part_id.
        payload: {__stream__: part_events, type: A, status: arrived}
        id_field: part_id
    ```

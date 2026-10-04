# Local Simulation

A production line that prints its events and telemetry to the terminal. It needs no container, so it is the place to start. Each tab shows the example written one way: with the declarative API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

=== "Declarative"

    This example demonstrates how to build a dynamic simulation using the declarative **Standard API (`SimulationContext`)** and **Local Connectors**.

    Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing and benchmarking. This example adds `ConsoleEgress` and no ingress, so the lathe keeps the capacity it starts with for the whole run. Its low-level twin, in the Low-level tab, shows how `LocalIngress` schedules parameter changes at set times.

    **Quick Start**

    Download the script, then run it. This example needs no container.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/local_example.py
    ```

    **With uv**

    ```bash
    # 1. Run the declarative simulation
    uv run --no-project --with dynamic-des local_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package
    pip install dynamic-des

    # 2. Run the declarative simulation
    python local_example.py
    ```

    **Full Source Code**

    This script initializes a production line, runs it for 60 simulation seconds, and streams events and telemetry directly to your terminal.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/local_example.py"
    """Local simulation, declarative API, no containers.

    The smallest complete example. `Factory_A` is built with `SimulationContext` and writes
    to `ConsoleEgress`, so events and telemetry are printed to the terminal and nothing
    external is involved.

    Start here. It needs no broker, no database and no object store, and it ends on its own
    after 60 simulation seconds.
    """

    import logging

    from dynamic_des import ConsoleEgress, SimulationContext

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)

    # ==========================================
    # 1. Declarative Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(sim_id="Factory_A", factor=1.0)
        .add_egress(ConsoleEgress())
        .add_resource("lathe", current_cap=2, max_cap=5)
        .add_service("milling", dist="normal", mean=3.0, std=0.5)
        .add_arrival("standard", dist="exponential", rate=1.0)
    )


    # ==========================================
    # 2. Simulation Logic (Decorators)
    # ==========================================
    @app.task(service_id="milling", resource_id="lathe")
    def process_part(task_id: int):
        """Executes the milling service and returns the custom payload."""
        return {"event_type": "part_produced", "part_id": task_id, "quality": "A"}


    @app.arrival_loop("standard")
    def arrival_generator(context):
        """Continuously spawns new parts based on the 'standard' arrival distribution."""
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(process_part(task_id))
            task_id += 1


    @app.telemetry_loop(interval=2.0)
    def telemetry_generator(context):
        """Samples the hidden state of the resources every 2 simulation seconds."""
        res = context.get_resource("lathe")
        util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0

        context.publish("utilization", util)
        context.publish("queue_length", len(res.queue.items))


    # ==========================================
    # 3. Execution
    # ==========================================
    def run():
        """Starts the local simulation for a fixed duration."""
        logger.info("Starting Declarative Local Example. Running for 60 seconds...")
        app.run(until=60)


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    This example demonstrates how to build a dynamic simulation using the low-level **Imperative API** and **Local Connectors**.

    Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals.

    **Quick Start**

    Download the script, then run it. This example needs no container.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/local_example.py
    ```

    **With uv**

    ```bash
    # 1. Run the imperative simulation
    uv run --no-project --with dynamic-des local_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package
    pip install dynamic-des

    # 2. Run the imperative simulation
    python local_example.py
    ```

    **Full Source Code**

    This script initializes a production line and runs it for 30 simulation seconds. `LocalIngress` schedules two capacity changes: the lathe goes from 1 to 3 at t=10s, then down to 2 at t=20s. Events and telemetry stream directly to your terminal, driven by raw SimPy generators.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/imperative/local_example.py"
    """Local simulation with scheduled parameter changes, imperative API.

    The low-level twin of `declarative/local_example.py`, built on
    `DynamicRealtimeEnvironment` directly. It adds what the declarative version does not
    have: `LocalIngress` schedules two capacity changes, so the lathe goes from 1 to 3 at
    t=10s and down to 2 at t=20s, and the telemetry shows the effect.

    Needs no containers. Ends on its own after 30 simulation seconds.
    """

    import logging

    import numpy as np

    from dynamic_des import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        LocalIngress,
        Sampler,
        SimParameter,
    )

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    logger = logging.getLogger("local_example")


    def run():
        # 1. Define the system schema
        # Line_A starts with 1 lathe, but has a physical ceiling of 5.
        line_a_params = SimParameter(
            sim_id="Line_A",
            arrival={"standard": DistributionConfig(dist="exponential", rate=1.0)},
            service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
            resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
        )

        # 2. Setup Environment with Local Connectors
        # Schedule capacity updates: jump to 3 at t=10s, then drop to 2 at t=20s
        ingress = LocalIngress(
            schedule=[
                (10.0, "Line_A.resources.lathe.current_cap", 3),
                (20.0, "Line_A.resources.lathe.current_cap", 2),
            ]
        )
        egress = ConsoleEgress()

        env = DynamicRealtimeEnvironment(factor=1.0)
        env.registry.register_sim_parameter(line_a_params)
        env.setup_ingress([ingress])
        env.setup_egress([egress])

        # 3. Initialize Resources and Sampler
        res = DynamicResource(env, "Line_A", "lathe")
        sampler = Sampler(rng=np.random.default_rng(42))

        # 4. Define Simulation Logic
        def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
            """Generates tasks based on the dynamic arrival rate."""
            arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
            service_path = "Line_A.service.milling"
            task_id = 0

            while True:
                # Reference-based: arrival_cfg updates automatically via Registry
                yield env.timeout(sampler.sample(arrival_cfg))
                env.process(work_task(env, task_id, res, service_path))
                task_id += 1

        def work_task(
            env: DynamicRealtimeEnvironment,
            task_id: int,
            res: DynamicResource,
            path_id: str,
        ):
            """Models task lifecycle: queued -> started -> finished."""
            task_key = f"task-{task_id}"
            env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

            with res.request() as req:
                yield req
                # Late Binding: Fetch latest config only when work actually starts
                current_service_cfg = env.registry.get_config(path_id)

                env.publish_event(task_key, {"path_id": path_id, "status": "started"})

                yield env.timeout(sampler.sample(current_service_cfg))
                env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

        def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
            """Streams system health metrics every 2 seconds."""
            while True:
                env.publish_telemetry("Line_A.resources.lathe.capacity", res.capacity)
                env.publish_telemetry("Line_A.resources.lathe.in_use", res.in_use)
                env.publish_telemetry(
                    "Line_A.resources.lathe.queue_length", len(res.queue.items)
                )

                util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
                env.publish_telemetry("Line_A.resources.lathe.utilization", util)
                yield env.timeout(2.0)

        # 5. Run the Simulation
        env.process(arrival_process(env, res))
        env.process(telemetry_monitor(env, res))

        logging.info("Simulation started. Watch capacity change at t=10.0s and 20.0s...")
        try:
            env.run(until=30)
        finally:
            env.teardown()


    if __name__ == "__main__":
        run()
    ```

=== "YAML"

    This example builds the same simulation as the declarative example in the Declarative tab, from a YAML blueprint and nothing else. No Python is written: the arrival loop, the task and the telemetry loop are all declared in the file.

    It writes to `ConsoleEgress` and needs no container, so it is the place to start.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/local.yaml
    ```

    **With uv**

    ```bash
    # 1. Run the blueprint
    uv run --no-project --with dynamic-des ddes run local.yaml
    ```

    **With pip**

    ```bash
    # 1. Install the package
    pip install dynamic-des

    # 2. Run the blueprint
    ddes run local.yaml
    ```

    **What It Does**

    The run prints every record to the terminal and stops after 60 simulation seconds, which at `factor: 1.0` is one real minute. Telemetry lines carry `[TEL]` and events carry `[EVT]`:

    ```text
    [TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'Factory_A.utilization', 'value': 0.0}
    [EVT] {'sim_ts': 0.328, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.milling', 'status': 'queued'}}
    [EVT] {'sim_ts': 3.754, 'timestamp': '...', 'key': 'task-0', 'value': {'event_type': 'part_produced', 'quality': 'A', 'part_id': 0}}
    ```

    Each part produces a `queued`, a `started` and a finished event. The finished event is the task's `payload`, with the task id added as `part_id` because of `id_field`. Add `--until 10` to stop after 10 simulation seconds instead.

    **Full Source Code**

    The blueprint declares one resource, one service and one arrival. `spawn` names the task each arrival starts, and the `telemetry` entry publishes two statistics of the lathe every 2 simulation seconds.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/local.yaml"
    # Local simulation in YAML, with no Python and no containers.
    #
    # The twin of examples/declarative/local_example.py. Factory_A writes to
    # ConsoleEgress, so events and telemetry are printed to the terminal, and the run
    # ends on its own after 60 simulation seconds.
    #
    # Run it with: ddes run examples/yaml/local.yaml

    simulation:
      sim_id: Factory_A
      factor: 1.0

    egress:
      - type: Console

    resources:
      lathe: {current_cap: 2, max_cap: 5}

    services:
      milling: {dist: normal, mean: 3.0, std: 0.5}

    arrivals:
      # Each arrival spawns one process_part task.
      standard: {dist: exponential, rate: 1.0, spawn: process_part}

    tasks:
      process_part:
        service: milling
        resource: lathe
        # The value of the task's finished event. id_field adds the task id as part_id.
        payload: {event_type: part_produced, quality: A}
        id_field: part_id

    telemetry:
      # Samples the lathe every 2 simulation seconds.
      - interval: 2.0
        publish:
          utilization: lathe.utilization
          queue_length: lathe.queue_length

    run:
      until: 60
    ```

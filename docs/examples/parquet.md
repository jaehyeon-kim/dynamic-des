# Fast-Forward to Parquet

A week of a production line, generated at `factor=0.0` and written to Parquet files on the local disk or in S3-compatible storage. Each tab shows the example written one way: with the declarative API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

=== "Declarative"

    While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

    This example demonstrates how to run a simulation in **fast-forward mode** using the declarative **Standard API (`SimulationContext`)** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

    **Quick Start**

    Download the script, then run it. The run writes Parquet chunks to a local `data/` folder by default, so no infrastructure is needed.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/parquet_example.py
    ```

    **With uv**

    ```bash
    # 1. Run the simulation
    uv run --no-project --with "dynamic-des[parquet]" parquet_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the parquet extra
    pip install "dynamic-des[parquet]"

    # 2. Run the simulation
    python parquet_example.py
    ```

    To write to S3 instead, start the object store and set `USE_S3`. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>. `odctl` comes from `uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`.

    ```bash
    # 1. Spin up SeaweedFS with odctl
    odctl up storage

    # 2. Run the simulation against S3, with uv
    USE_S3=true uv run --no-project --with "dynamic-des[parquet]" parquet_example.py

    #    ...or with pip
    USE_S3=true python parquet_example.py

    # 3. Clean up the infrastructure when finished
    odctl down storage --volumes
    ```

    `DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the destination and credentials.

    **Full Source Code**

    This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out instantly.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/parquet_example.py"
    """
    Historical Data Generation Example.

    Demonstrates using SimulationContext as a fast-forward data engine.
    By setting `factor=0.0`, the SimPy clock detaches from wall-clock time,
    executing the exact same factory logic instantly to generate massive
    historical datasets for Machine Learning models via Parquet/S3.
    """

    import logging
    import os
    from datetime import datetime, timedelta

    from dynamic_des import ParquetStorageEgress, SimulationContext

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)


    def create_history_router(base_path: str):
        """
        Router Factory: Generates a router function injected with the correct
        base path, and flattens nested event payloads for Parquet.
        """

        def history_router(data: dict) -> str | None:
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None

            stream_type = data.get("stream_type")

            if stream_type == "telemetry":
                return None

            # FLATTEN EVENT FOR PARQUET
            if (
                stream_type == "event"
                and "value" in data
                and isinstance(data["value"], dict)
            ):
                nested_value = data.pop("value")
                data.update(nested_value)

            return f"{base_path}/events.parquet"

        return history_router


    # ==========================================
    # 1. DUAL-MODE STORAGE CONFIGURATION
    # ==========================================
    # Reading environment variables and constructing the S3 client are safe at import.
    # Creating the destination is not, so it lives in ensure_destination() and runs from
    # run(). Importing this module for discovery, by a test collector or a docs build,
    # must not create a directory or reach out to S3.
    use_s3 = os.getenv("USE_S3", "false").lower() == "true"
    # odctl-dev is one of the buckets the odctl `storage` profile creates.
    base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
    filesystem = None

    if use_s3:
        from pyarrow import fs

        filesystem = fs.S3FileSystem(
            access_key=os.getenv("S3_ACCESS_KEY", "user"),
            secret_key=os.getenv("S3_SECRET_KEY", "password"),
            endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
            scheme="http",
        )


    def ensure_destination() -> None:
        """Create the target directory or bucket path, once the caller has asked to run."""
        if use_s3 and filesystem is not None:
            logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
            filesystem.create_dir(base_path)
        else:
            logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
            os.makedirs(base_path, exist_ok=True)


    router = create_history_router(base_path)

    # The week of backdating this example exists to demonstrate. It sits at module scope
    # because the builder below needs it, and the builder has to stay at module scope for
    # the decorators further down to attach to it. The imperative twin computes the same
    # value inside run(), where it has no such constraint.
    LOGICAL_START_TIME = datetime.now() - timedelta(days=7)

    # ==========================================
    # 2. Declarative Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(
            sim_id="Line_A",
            factor=0.0,
            random_seed=42,
            # Without this the example logs that it is backdating a week and then timestamps
            # every record from the current clock, which is the opposite of what it claims.
            logical_start_time=LOGICAL_START_TIME,
        )
        .add_egress(ParquetStorageEgress(path_router=router, filesystem=filesystem))
        .with_batching(batch_size=5000, flush_interval=86400)
        .add_resource("lathe", current_cap=4, max_cap=10)
        .add_service("milling", dist="normal", mean=2.0, std=0.2)
        .add_arrival("standard", dist="exponential", rate=2.0)
    )


    # ==========================================
    # 3. Simulation Logic
    # ==========================================
    @app.task(service_id="milling", resource_id="lathe")
    def process_part(task_id: int, context):
        """
        Returns the exact flat dictionary expected by the parquet router
        to represent the 'finished' state of the lifecycle.
        """
        return {"path_id": "Line_A.service.milling", "status": "finished"}


    @app.arrival_loop("standard")
    def arrival_generator(context):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(process_part(task_id, context))
            task_id += 1


    @app.telemetry_loop(interval=60.0)
    def telemetry_generator(context):
        """Samples the hidden state of the resources every 60 simulation seconds."""
        res = context.get_resource("lathe")

        context.publish("lathe.capacity", res.capacity)
        context.publish("lathe.in_use", res.in_use)
        context.publish("lathe.queue_length", len(res.queue.items))

        util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
        context.publish("lathe.utilization", util)


    # ==========================================
    # 4. Execution
    # ==========================================
    def run():
        """Generates 1 week of factory data instantly."""
        ensure_destination()

        logger.info(
            "Generating historical data mimicking start from "
            f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info("Fast-forwarding (factor=0.0)...")

        # The clock detaches and executes 1 week of operations instantaneously
        app.run(until="1 week")

        logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

    This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Imperative API** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

    **Quick Start**

    Download the script, then run it. The run writes Parquet chunks to a local `data/` folder by default, so no infrastructure is needed.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/parquet_example.py
    ```

    **With uv**

    ```bash
    # 1. Run the simulation
    uv run --no-project --with "dynamic-des[parquet]" parquet_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the parquet extra
    pip install "dynamic-des[parquet]"

    # 2. Run the simulation
    python parquet_example.py
    ```

    To write to S3 instead, start the object store and set `USE_S3`. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>. `odctl` comes from `uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`.

    ```bash
    # 1. Spin up SeaweedFS with odctl
    odctl up storage

    # 2. Run the simulation against S3, with uv
    USE_S3=true uv run --no-project --with "dynamic-des[parquet]" parquet_example.py

    #    ...or with pip
    USE_S3=true python parquet_example.py

    # 3. Clean up the infrastructure when finished
    odctl down storage --volumes
    ```

    `DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the destination and credentials.

    **Full Source Code**

    This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/imperative/parquet_example.py"
    """Historical data generation, imperative API.

    The low-level twin of `declarative/parquet_example.py`. It wires
    `DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
    through the builder, which shows what `SimulationContext` does for you.

    `factor=0.0` detaches the clock from real time, so seven days of history are generated
    as fast as the machine allows and written to Parquet. A router keeps lifecycle events
    and drops telemetry. Writes to `data/` unless `USE_S3=true`.
    """

    import logging
    import os
    from datetime import datetime, timedelta

    import numpy as np

    from dynamic_des import (
        CapacityConfig,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        ParquetStorageEgress,
        Sampler,
        SimParameter,
    )
    from dynamic_des.utils import time_to_seconds

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    logger = logging.getLogger("parquet_example")


    def create_history_router(base_path: str):
        """
        Router Factory: Generates a router function injected with the correct
        base path, and flattens nested event payloads for Parquet.
        """

        def history_router(data: dict) -> str | None:
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None

            stream_type = data.get("stream_type")

            if stream_type == "telemetry":
                return None

            # FLATTEN EVENT FOR PARQUET
            # If it's an event and has a nested 'value' dictionary, flatten it
            if (
                stream_type == "event"
                and "value" in data
                and isinstance(data["value"], dict)
            ):
                # Extract and remove the nested 'value' object
                nested_value = data.pop("value")
                # Merge the nested keys (path_id, status) directly into the root dict
                data.update(nested_value)

            return f"{base_path}/events.parquet"

        return history_router


    def run():
        # ---------------------------------------------------------
        # 1. DUAL-MODE STORAGE CONFIGURATION
        # ---------------------------------------------------------
        use_s3 = os.getenv("USE_S3", "false").lower() == "true"
        # odctl-dev is one of the buckets the odctl `storage` profile creates.
        base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
        filesystem = None

        if use_s3:
            # Lazy import PyArrow so the script doesn't crash if running purely local
            # without the [parquet] extra installed (though it is needed for ParquetEgress)
            from pyarrow import fs

            logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
            filesystem = fs.S3FileSystem(
                access_key=os.getenv("S3_ACCESS_KEY", "user"),
                secret_key=os.getenv("S3_SECRET_KEY", "password"),
                endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
                scheme="http",
            )
            # Ensure S3 Bucket exists
            filesystem.create_dir(base_path)
        else:
            logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
            # Ensure local directory exists
            os.makedirs(base_path, exist_ok=True)

        # ---------------------------------------------------------
        # 2. SIMULATION SETUP
        # ---------------------------------------------------------
        line_a_params = SimParameter(
            sim_id="Line_A",
            arrival={"standard": DistributionConfig(dist="exponential", rate=2.0)},
            service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
            resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
        )

        start_time = datetime.now() - timedelta(days=7)
        env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
        env.registry.register_sim_parameter(line_a_params)

        # Initialize Egress with the dynamic router and conditional filesystem
        router = create_history_router(base_path)
        egress = ParquetStorageEgress(path_router=router, filesystem=filesystem)

        # batch_size=5000 for compression, flush_interval=86400 (1 day in sim time)
        env.setup_egress([egress], batch_size=5000, flush_interval=86400)

        res = DynamicResource(env, "Line_A", "lathe")
        sampler = Sampler(rng=np.random.default_rng(42))

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
            env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

            with res.request() as req:
                yield req
                current_service_cfg = env.registry.get_config(path_id)
                env.publish_event(task_key, {"path_id": path_id, "status": "started"})
                yield env.timeout(sampler.sample(current_service_cfg))
                env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

        def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
            while True:
                env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
                env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
                env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

                util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
                env.publish_telemetry("Line_A.lathe.utilization", util)

                yield env.timeout(60.0)

        env.process(arrival_process(env, res))
        env.process(telemetry_monitor(env, res))

        run_duration_str = "1 week"
        run_duration_sec = time_to_seconds(run_duration_str)

        logger.info(
            f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info("Fast-forwarding (factor=0.0)...")

        try:
            env.run(until=run_duration_sec)
        finally:
            env.teardown()
            logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


    if __name__ == "__main__":
        run()
    ```

=== "YAML"

    This example builds the simulation of the declarative example in the Declarative tab from a YAML blueprint, with no Python. With `factor: 0.0` the clock is detached from the wall clock, so a week of factory data is written to Parquet as fast as the machine allows.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/parquet.yaml
    ```

    **With uv**

    ```bash
    # 1. Run the blueprint
    uv run --no-project --with "dynamic-des[parquet]" ddes run parquet.yaml
    ```

    **With pip**

    ```bash
    # 1. Install the package with the parquet extra
    pip install "dynamic-des[parquet]"

    # 2. Run the blueprint
    ddes run parquet.yaml
    ```

    **What It Does**

    The run writes Parquet chunks to a local `data/` folder by default and ends on its own. The folder is created on the first write. On a laptop a week of simulated time takes about 20 seconds and writes about 850 files with 3.6 million rows, one row per lifecycle event. Without a router, the egress drops telemetry and writes each event as one flat row, so the columns are `stream_type`, `sim_ts`, `timestamp`, `key`, `path_id` and `status`. `logical_start_time: -7d` stamps the week as history that ends now.

    To write to S3 instead, run `odctl up storage` and set the variables the file reads:

    ```bash
    PARQUET_FILESYSTEM=s3 DEST_PATH=odctl-dev/history S3_ENDPOINT=http://localhost:8333 \
      S3_ACCESS_KEY=user S3_SECRET_KEY=password ddes run parquet.yaml
    ```

    The chunks land under the `odctl-dev/history/` prefix.

    **Full Source Code**

    `filesystem` is a mapping. `type` picks the local disk or S3, and the other keys are passed to PyArrow's `S3FileSystem`. A key whose value is empty is left out, so with no variables set the mapping is the local disk.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/parquet.yaml"
    # Historical data generation in YAML.
    #
    # The YAML version of examples/declarative/parquet_example.py. With factor 0 the
    # clock is detached from the wall clock, so a week of factory data is written to
    # Parquet as fast as the machine allows. Without a router, each event is written as
    # one flat row and telemetry is left out. The folder is created on the first write.
    #
    # Writes to ./data by default. To write to the odctl storage profile instead (odctl
    # up storage), set PARQUET_FILESYSTEM=s3, DEST_PATH=odctl-dev/history,
    # S3_ENDPOINT=http://localhost:8333, S3_ACCESS_KEY=user and S3_SECRET_KEY=password.
    # Run it with: ddes run examples/yaml/parquet.yaml

    simulation:
      sim_id: Line_A
      factor: 0.0
      random_seed: 42
      # A week before now, so the records are stamped as history.
      logical_start_time: -7d

    egress:
      - type: Parquet
        config:
          default_path: ${DEST_PATH:-data}/events.parquet
          # Empty values are left out, so with no variables set this is the local disk.
          filesystem:
            type: ${PARQUET_FILESYSTEM:-local}
            endpoint_override: ${S3_ENDPOINT:-}
            access_key: ${S3_ACCESS_KEY:-}
            secret_key: ${S3_SECRET_KEY:-}

    batching:
      batch_size: 5000
      flush_interval: 86400

    resources:
      lathe: {current_cap: 4, max_cap: 10}

    services:
      milling: {dist: normal, mean: 2.0, std: 0.2}

    arrivals:
      standard: {dist: exponential, rate: 2.0, spawn: process_part}

    tasks:
      process_part:
        service: milling
        resource: lathe
        payload: {path_id: Line_A.service.milling, status: finished}

    run:
      until: 1 week
    ```

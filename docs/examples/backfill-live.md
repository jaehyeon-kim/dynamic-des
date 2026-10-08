# Backfill Then Go Live

One run writes ten minutes of backdated history to Parquet, then publishes to Kafka in real time. [Backfill Then Go Live in One Run](../guides/backfill-then-live.md) explains how the two instants work. Each tab shows the example written one way. There is no low-level version.

=== "Declarative"

    [`examples/declarative/backfill_live_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/backfill_live_example.py) backdates the clock by ten minutes, writes those ten minutes to Parquet in well under a second, then publishes to Kafka in real time for sixty seconds. `HISTORY_MINUTES` and `LIVE_SECONDS` set the two halves.

    **Quick Start**

    Download the script, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/backfill_live_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=1.0,<2"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Backfill ten minutes of history to Parquet, generated instantly rather than
    #    waited for, then tail live to Kafka for sixty seconds
    uv run --no-project --with "dynamic-des[kafka,parquet]" backfill_live_example.py

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the kafka,parquet extra, and odctl for the containers
    pip install "dynamic-des[kafka,parquet]" "odctl>=1.0,<2"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Backfill ten minutes of history to Parquet, generated instantly rather than
    #    waited for, then tail live to Kafka for sixty seconds
    python backfill_live_example.py

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    History finishes within the same second the run starts, then nothing is logged at all until teardown, because from `go_live_at` the run waits on the wall clock exactly as a live twin does. `Logical clock reached go-live` is the line that confirms the switch.

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/backfill_live_example.py"
    """
    Backfill-then-live Example.

    One run produces both halves of a tiered dataset. Until `go_live_at` the clock is
    detached from the wall clock, so ten minutes of backdated history are written to
    Parquet as fast as the machine allows. From `go_live_at` the same run is paced at one
    simulated second per real second, and the same events are published to Kafka as they
    happen. Doing this in two processes would mean repeating the seed and the start
    instant in both, and keeping them in step by hand.
    """

    import logging
    import os
    import time
    from datetime import datetime, timedelta

    from dynamic_des import (
        KafkaAdminConnector,
        KafkaEgress,
        ParquetStorageEgress,
        SimulationContext,
    )

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    EVENT_TOPIC = "sim-events"
    TELEMETRY_TOPIC = "sim-telemetry"

    # How much history to generate, and how long to keep tailing once live. The live half
    # costs real time, second for second, so it is short by default.
    HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
    LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))

    base_path = os.getenv("DEST_PATH", "data/backfill")

    # The go-live instant is now, so everything before it is history and everything after
    # it is the live tail. These sit at module scope because the builder below needs them,
    # and the builder has to stay at module scope for the decorators to attach to it.
    GO_LIVE_AT = datetime.now()
    LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

    # Records carry their logical time as an ISO string, so the predicates that split the
    # two sinks compare strings. That works because every timestamp comes from the same
    # formatter: identical layout, so ordering by text is ordering by time.
    GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")


    def is_history(record: dict) -> bool:
        """True for records stamped before the go-live instant."""
        return record["timestamp"] < GO_LIVE_ISO


    def is_live(record: dict) -> bool:
        """True for records stamped at or after the go-live instant."""
        return record["timestamp"] >= GO_LIVE_ISO


    def history_router(data: dict) -> str | None:
        """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
        if data.get("stream_type") != "event":
            return None

        if isinstance(data.get("value"), dict):
            data.update(data.pop("value"))

        return f"{base_path}/events.parquet"


    # ==========================================
    # 1. Declarative Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(
            sim_id="Line_A",
            # Unpaced to begin with, so the history costs no real time.
            factor=0.0,
            random_seed=42,
            logical_start_time=LOGICAL_START_TIME,
            # From here the same run is paced at one simulated second per real second.
            go_live_at=GO_LIVE_AT,
        )
        .add_egress(ParquetStorageEgress(path_router=history_router), when=is_history)
        .add_egress(
            KafkaEgress(
                event_topic=EVENT_TOPIC,
                telemetry_topic=TELEMETRY_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
            ),
            when=is_live,
        )
        # Only batch_size governs the history. The interval flush is a simulation process,
        # started only when factor is non-zero as the egress is set up, and this run starts
        # at 0.0. It starts at go-live, so the live tail also flushes every 10 seconds.
        .with_batching(batch_size=2000, flush_interval=10.0)
        .add_resource("lathe", current_cap=4, max_cap=10)
        .add_service("milling", dist="normal", mean=2.0, std=0.2)
        .add_arrival("standard", dist="exponential", rate=0.5)
    )


    # ==========================================
    # 2. Simulation Logic
    # ==========================================
    @app.task(service_id="milling", resource_id="lathe")
    def process_part(task_id: int, context):
        """Returns the flat payload that both sinks receive for a finished task."""
        return {"path_id": "Line_A.service.milling", "status": "finished"}


    @app.arrival_loop("standard")
    def arrival_generator(context):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(process_part(task_id, context))
            task_id += 1


    @app.telemetry_loop(interval=30.0)
    def telemetry_generator(context):
        """Samples resource use every 30 simulation seconds."""
        res = context.get_resource("lathe")

        context.publish("lathe.in_use", res.in_use)
        context.publish("lathe.queue_length", len(res.queue.items))


    # ==========================================
    # 3. Execution
    # ==========================================
    def run():
        """Generates the history instantly, then tails live for LIVE_SECONDS."""
        os.makedirs(base_path, exist_ok=True)

        try:
            admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
            admin.create_topics(
                topics_config=[
                    {"name": EVENT_TOPIC, "partitions": 1},
                    {"name": TELEMETRY_TOPIC, "partitions": 1},
                ]
            )
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Could not explicitly create topics: {e}")

        logger.info(
            "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
            LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
            GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
            base_path,
            LIVE_SECONDS,
        )

        app.run(until=HISTORY.total_seconds() + LIVE_SECONDS)

        logger.info("Run complete. History is in '%s/', the tail is in Kafka.", base_path)


    if __name__ == "__main__":
        run()
    ```

=== "YAML"

    This example builds the simulation of the Declarative tab from a YAML blueprint, with no Python. Until `go_live_at` the run is unpaced and writes backdated history to Parquet. From `go_live_at` it is paced in real time and publishes to Kafka.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/backfill_live.yaml
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=1.0,<2"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the blueprint
    uv run --no-project --with "dynamic-des[kafka,parquet]" ddes run backfill_live.yaml

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the kafka,parquet extra, and odctl for the containers
    pip install "dynamic-des[kafka,parquet]" "odctl>=1.0,<2"

    # 2. Start the Kafka broker and schema registry
    odctl up kafka-lite

    # 3. Run the blueprint
    ddes run backfill_live.yaml

    # 4. Clean up the infrastructure when finished
    odctl down kafka-lite --volumes
    ```

    **What It Does**

    Ten minutes of history are written to `data/backfill/` within the first second, then the run publishes to `sim-events` and `sim-telemetry` in real time for 60 seconds and ends. The history folder is created on the first write, and `KafkaEgress` creates the two topics when it starts.

    **Full Source Code**

    `logical_start_time: -10m` and `go_live_at: now` are read against one moment, when the file is loaded, so they are exactly ten minutes apart, and `until: 11m` adds one minute of live tail. `when: history` sends the records stamped before `go_live_at` to Parquet, and `when: live` sends the rest to Kafka.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/backfill_live.yaml"
    # Backfill then go live in one run, in YAML.
    #
    # The YAML version of examples/declarative/backfill_live_example.py. Until
    # go_live_at the clock is detached from the wall clock, so ten minutes of backdated
    # history are written to Parquet as fast as the machine allows. From go_live_at the
    # run is paced at one simulated second per real second, and the same events go to
    # Kafka for one minute.
    #
    # Needs a broker: odctl up kafka-lite.
    # Run it with: ddes run examples/yaml/backfill_live.yaml

    simulation:
      sim_id: Line_A
      # Unpaced to begin with, so the history costs no real time.
      factor: 0.0
      random_seed: 42
      logical_start_time: -10m
      # From here the same run is paced at one simulated second per real second.
      go_live_at: now

    egress:
      - type: Parquet
        config:
          default_path: data/backfill/events.parquet
        # Records stamped before go_live_at.
        when: history
      - type: Kafka
        config:
          event_topic: sim-events
          telemetry_topic: sim-telemetry
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
        # Records stamped at or after go_live_at.
        when: live

    # Only batch_size governs the history. The interval flush starts at go-live, so the
    # live tail also flushes every 10 seconds.
    batching:
      batch_size: 2000
      flush_interval: 10.0

    resources:
      lathe: {current_cap: 4, max_cap: 10}

    services:
      milling: {dist: normal, mean: 2.0, std: 0.2}

    arrivals:
      standard: {dist: exponential, rate: 0.5, spawn: process_part}

    tasks:
      process_part:
        service: milling
        resource: lathe
        payload: {path_id: Line_A.service.milling, status: finished}

    telemetry:
      - interval: 30.0
        publish:
          lathe.in_use: lathe.in_use
          lathe.queue_length: lathe.queue_length

    run:
      # Ten minutes of history, then one minute of live tail.
      until: 11m
    ```

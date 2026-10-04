# Relational DB (Postgres)

A store that writes customer orders to PostgreSQL and takes parameter updates from a PostgreSQL table. Each tab shows the example written one way: with the declarative API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

=== "Declarative"

    This example demonstrates how to integrate `dynamic-des` into a relational database architecture using the declarative **Standard API (`SimulationContext`)**.

    By replacing the local console egress with `PostgresEgress`, the simulation becomes a fully detached data generator. It automatically streams complex, interrelated data (like parent `orders` and child `order_items`) directly into your PostgreSQL database using high-performance, asynchronous bulk inserts via `asyncpg`.

    **1. Multiplexing Multiple Tables**

    When generating complex relational data, a single event loop often produces records that belong in completely different SQL tables.

    Instead of creating separate queues or complex routing logic, `PostgresEgress` supports **Table Multiplexing**. By attaching multiple instances of `PostgresEgress` to the simulation and tagging your output dictionaries with a special `__table__` key, the framework automatically filters and routes the records to their correct destinations.

    ```python
    app = (
        SimulationContext(sim_id="Store", factor=1.0)
        .add_egress(PostgresEgress(DSN, table_name="orders"))
        .add_egress(PostgresEgress(DSN, table_name="order_items"))
    )
    ```

    **2. Generating Interrelated Data**

    Inside the generator loop, you can yield a single `order`, followed immediately by multiple `order_items` that reference the same parent `order_id`. Notice the `__table__` key embedded in the dictionaries:

    ```python
    # Generate Order Parent Record
    order = {
        "__table__": "orders",
        "order_id": 1,
        "total_amount": 100.0,
    }
    context.publish("order_event", order)

    # Generate Order Item Child Record
    item = {
        "__table__": "order_items",
        "order_item_id": 1,
        "order_id": 1,      # References parent
        "unit_price": 50.0
    }
    context.publish("order_event", item)
    ```

    **3. Dynamic Parameter Updates (Ingress)**

    This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation is running, you can dynamically update parameters (like speeding up the order arrival rate) simply by executing an `INSERT` statement in your database! The simulation fetches the new configuration within its poll interval, 2 seconds by default.

    **4. Quick Start**

    Download the script, then run it. The run keeps generating orders until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the SQL command below.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/postgres_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Spin up the Postgres database
    odctl up postgres

    # 3. Run the declarative simulation
    uv run --no-project --with "dynamic-des[postgres]" postgres_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the postgres extra, and odctl for the containers
    pip install "dynamic-des[postgres]" "odctl>=0.5.1"

    # 2. Spin up the Postgres database
    odctl up postgres

    # 3. Run the declarative simulation
    python postgres_example.py
    ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the database container and inject the parameter (maintains audit history!)
    docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
    ```
    *You will immediately see the simulation terminal log that the update was ingested and start generating orders much faster!*

    ```bash
    # Clean up the infrastructure when finished
    odctl down postgres --volumes
    ```

    **Full Source Code**

    This script connects the simulation to PostgreSQL, automatically initializes the database schema, and generates continuous streams of interrelated commerce data.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/declarative/postgres_example.py"
    """Relational output with table multiplexing, declarative API.

    `Store` writes interrelated commerce data to PostgreSQL. Two `PostgresEgress` instances
    are attached, one per table, and each keeps only the records whose `__table__` key
    matches its own `table_name`, so one event stream fills `orders` and `order_items`
    correctly. `PostgresIngress` polls `simulation_params` for parameter updates.

    Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
    """

    import asyncio
    import logging
    import random
    from datetime import datetime

    import asyncpg

    from dynamic_des import PostgresEgress, PostgresIngress, SimulationContext

    logger = logging.getLogger(__name__)

    # Connection string matching the odctl `postgres` profile
    DSN = "postgresql://user:password@localhost:5432/odctl"

    app = (
        SimulationContext(sim_id="Store", factor=1.0)
        .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
        .add_egress(PostgresEgress(DSN, table_name="orders"))
        .add_egress(PostgresEgress(DSN, table_name="order_items"))
        .add_arrival("customer_order", dist="exponential", rate=1.0)
    )


    @app.arrival_loop("customer_order")
    def order_generator(context):
        order_id = 1
        item_id = 1
        while True:
            yield context.wait_for_arrival("customer_order")

            # 1. Generate Order (Notice the __table__ routing key)
            order = {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": random.randint(1, 100),
                "order_date": datetime.utcnow().isoformat(),
                "total_amount": 0.0,
                "status": "pending",
            }

            # 2. Generate 1 to 5 Order Items
            num_items = random.randint(1, 5)
            total = 0.0

            for _ in range(num_items):
                price = round(random.uniform(10.0, 50.0), 2)
                qty = random.randint(1, 3)
                total += price * qty

                item = {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": random.randint(1, 50),
                    "quantity": qty,
                    "unit_price": price,
                }
                context.publish("order_event", item)
                item_id += 1

            order["total_amount"] = round(total, 2)
            context.publish("order_event", order)

            order_id += 1


    async def init_db():
        """Initializes the database schema before starting the simulation."""
        conn = await asyncpg.connect(DSN)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                customer_id INT,
                order_date TEXT,
                total_amount REAL,
                status TEXT
            );
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                unit_price REAL
            );
            CREATE TABLE IF NOT EXISTS simulation_params (
                id SERIAL PRIMARY KEY,
                param_path TEXT,
                param_value TEXT,
                is_applied BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Pre-seed the initial simulation parameters into the table as the baseline history only if empty
        count = await conn.fetchval("SELECT COUNT(*) FROM simulation_params")
        if count == 0:
            await conn.execute("""
                INSERT INTO simulation_params (param_path, param_value, is_applied)
                VALUES ('Store.arrival.customer_order.rate', '1.0', TRUE);
            """)
        await conn.close()
        logger.info("Database schema initialized and pre-seeded.")


    def run():
        logger.info("Starting Declarative Postgres Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
        )
        asyncio.run(init_db())
        try:
            app.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run()
    ```

=== "Low-level"

    This example demonstrates how to integrate `dynamic-des` into a relational database architecture using the low-level **Imperative API**.

    By replacing the local console egress with `PostgresEgress`, the simulation becomes a fully detached data generator. It automatically streams complex, interrelated data (like parent `orders` and child `order_items`) directly into your PostgreSQL database using high-performance, asynchronous bulk inserts via `asyncpg`.

    **1. Multiplexing Multiple Tables**

    When generating complex relational data, a single simulation process often produces records that belong in completely different SQL tables.

    Instead of creating separate queues or complex routing logic, `PostgresEgress` supports **Table Multiplexing**. By attaching multiple instances of `PostgresEgress` to the `DynamicRealtimeEnvironment` and tagging your output dictionaries with a special `__table__` key, the framework automatically filters and routes the records to their correct destinations.

    ```python
    egress_orders = PostgresEgress(DSN, table_name="orders")
    egress_items = PostgresEgress(DSN, table_name="order_items")

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.setup_egress([egress_orders, egress_items])
    ```

    **2. Generating Interrelated Data**

    Inside the raw SimPy process generator, you can yield a single `order`, followed immediately by multiple `order_items` that reference the same parent `order_id`. Notice the `__table__` key embedded in the dictionaries:

    ```python
    # Generate Order Parent Record
    order = {
        "__table__": "orders",
        "order_id": 1,
        "total_amount": 100.0,
    }
    env.publish_event("order-1", order)

    # Generate Order Item Child Record
    item = {
        "__table__": "order_items",
        "order_item_id": 1,
        "order_id": 1,      # References parent
        "unit_price": 50.0
    }
    env.publish_event("item-1", item)
    ```

    **3. Dynamic Parameter Updates (Ingress)**

    This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation is running, you can dynamically update parameters (like speeding up the order arrival rate) simply by executing an `INSERT` statement in your database! The simulation fetches the new configuration within its poll interval, 2 seconds by default.

    **4. Quick Start**

    Download the script, then run it. The run keeps generating orders until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the SQL command below.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/postgres_example.py
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Spin up the Postgres database
    odctl up postgres

    # 3. Run the imperative simulation
    uv run --no-project --with "dynamic-des[postgres]" postgres_example.py
    ```

    **With pip**

    ```bash
    # 1. Install the package with the postgres extra, and odctl for the containers
    pip install "dynamic-des[postgres]" "odctl>=0.5.1"

    # 2. Spin up the Postgres database
    odctl up postgres

    # 3. Run the imperative simulation
    python postgres_example.py
    ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the database container and inject the parameter (maintains audit history!)
    docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
    ```
    *You will immediately see the simulation terminal log that the update was ingested and start generating orders much faster!*

    ```bash
    # Clean up the infrastructure when finished
    odctl down postgres --volumes
    ```

    **Full Source Code**

    This script connects the simulation to PostgreSQL, automatically initializes the database schema, and generates continuous streams of interrelated commerce data.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/imperative/postgres_example.py"
    """Relational output with table multiplexing, imperative API.

    The low-level twin of `declarative/postgres_example.py`. Two `PostgresEgress` instances
    are attached, one per table, and each keeps only the records whose `__table__` key
    matches its own `table_name`.

    Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
    """

    import asyncio
    import logging
    import random
    from datetime import datetime

    import asyncpg
    import numpy as np

    from dynamic_des import (
        DistributionConfig,
        DynamicRealtimeEnvironment,
        PostgresEgress,
        PostgresIngress,
        Sampler,
        SimParameter,
    )

    logger = logging.getLogger("postgres_example")

    # Connection string matching the odctl `postgres` profile
    DSN = "postgresql://user:password@localhost:5432/odctl"


    async def init_db():
        """Initializes the database schema before starting the simulation."""
        conn = await asyncpg.connect(DSN)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                customer_id INT,
                order_date TEXT,
                total_amount REAL,
                status TEXT
            );
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                unit_price REAL
            );
            CREATE TABLE IF NOT EXISTS simulation_params (
                id SERIAL PRIMARY KEY,
                param_path TEXT,
                param_value TEXT,
                is_applied BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Pre-seed the initial simulation parameters into the table as the baseline history only if empty
        count = await conn.fetchval("SELECT COUNT(*) FROM simulation_params")
        if count == 0:
            await conn.execute("""
                INSERT INTO simulation_params (param_path, param_value, is_applied)
                VALUES ('Store.arrival.customer_order.rate', '1.0', TRUE);
            """)
        await conn.close()
        logger.info("Database schema initialized and pre-seeded.")


    def run():
        params = SimParameter(
            sim_id="Store",
            arrival={"customer_order": DistributionConfig(dist="exponential", rate=1.0)},
        )

        # Two egress instances, one per table. Each receives every record and keeps
        # only the rows whose __table__ matches its own table_name. They used to share
        # one queue and compete for batches, so whichever provider took a batch
        # discarded the other's rows from it.
        egress_orders = PostgresEgress(DSN, table_name="orders")
        egress_items = PostgresEgress(DSN, table_name="order_items")

        # Attach ingress for dynamic parameter updates
        ingress = PostgresIngress(DSN, table_name="simulation_params")

        env = DynamicRealtimeEnvironment(factor=1.0)
        env.registry.register_sim_parameter(params)
        env.setup_egress([egress_orders, egress_items])
        env.setup_ingress([ingress])

        sampler = Sampler(rng=np.random.default_rng(42))

        def order_process(env: DynamicRealtimeEnvironment):
            arrival_cfg = env.registry.get_config("Store.arrival.customer_order")
            order_id = 1
            item_id = 1

            while True:
                yield env.timeout(sampler.sample(arrival_cfg))

                # 1. Generate Order Parent Record
                order = {
                    "__table__": "orders",
                    "order_id": order_id,
                    "customer_id": random.randint(1, 100),
                    "order_date": datetime.utcnow().isoformat(),
                    "total_amount": 0.0,
                    "status": "pending",
                }

                # 2. Generate Order Item Children Records
                num_items = random.randint(1, 5)
                total = 0.0
                for _ in range(num_items):
                    price = round(random.uniform(10.0, 50.0), 2)
                    qty = random.randint(1, 3)
                    total += price * qty

                    item = {
                        "__table__": "order_items",
                        "order_item_id": item_id,
                        "order_id": order_id,
                        "product_id": random.randint(1, 50),
                        "quantity": qty,
                        "unit_price": price,
                    }
                    env.publish_event(f"item-{item_id}", item)
                    item_id += 1

                order["total_amount"] = round(total, 2)
                env.publish_event(f"order-{order_id}", order)

                order_id += 1

        env.process(order_process(env))

        logger.info("Starting Imperative Postgres Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
        )
        asyncio.run(init_db())

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

    This example builds a simulation like the declarative example in the Declarative tab from a YAML blueprint, with no Python and one table. Each customer order is written to `orders`, and `PostgresIngress` polls `simulation_params` for updates. The declarative example's orders with random line items are the [advanced Postgres example](advanced-postgres-orders.md), which keeps its generator in Python.

    **Quick Start**

    Download the blueprint, then run it.

    ```bash
    curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/postgres.yaml
    ```

    **With uv**

    ```bash
    # 1. Install odctl, which runs the containers
    uv tool install "odctl>=0.5.1"

    # 2. Start the Postgres database
    odctl up postgres

    # 3. Run the blueprint (Ctrl + C to stop)
    uv run --no-project --with "dynamic-des[postgres]" ddes run postgres.yaml

    # 4. Clean up the infrastructure when finished
    odctl down postgres --volumes
    ```

    **With pip**

    ```bash
    # 1. Install the package with the postgres extra, and odctl for the containers
    pip install "dynamic-des[postgres]" "odctl>=0.5.1"

    # 2. Start the Postgres database
    odctl up postgres

    # 3. Run the blueprint (Ctrl + C to stop)
    ddes run postgres.yaml

    # 4. Clean up the infrastructure when finished
    odctl down postgres --volumes
    ```

    **What It Does**

    When the run starts, `PostgresEgress` creates the `orders` table and `PostgresIngress` creates `simulation_params`, if they do not exist. The run then writes one order per arrival until you stop it. The order id is the task id, so a second run skips the orders the first one wrote, until the table is dropped.

    In a second terminal, raise the arrival rate while it runs:

    ```bash
    docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
    ```

    **Full Source Code**

    `place_order` has no service or resource, so it publishes its fixed `payload` as soon as an arrival spawns it, with the task id added as `order_id`. `tables` gives the columns and the primary key of each table to create. The `timestamp` column is filled from the time the record carries.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/postgres.yaml"
    # Relational output in YAML.
    #
    # The YAML version of examples/declarative/postgres_example.py, with one table.
    # Each customer_order spawns place_order, a task with no service or resource, which
    # publishes its payload at once. PostgresEgress creates the orders table at start
    # and writes one row per order. PostgresIngress creates simulation_params and polls
    # it for parameter updates.
    #
    # Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
    # Run it with: ddes run examples/yaml/postgres.yaml

    simulation:
      sim_id: Store
      factor: 1.0

    ingress:
      - type: Postgres
        config:
          connection_dsn: postgresql://user:password@localhost:5432/odctl
          table_name: simulation_params

    egress:
      - type: Postgres
        config:
          connection_dsn: postgresql://user:password@localhost:5432/odctl
          tables:
            orders:
              columns:
                order_id: INT
                customer_id: INT
                total_amount: REAL
                status: TEXT
                timestamp: TIMESTAMP
              primary_key: order_id

    arrivals:
      customer_order: {dist: exponential, rate: 1.0, spawn: place_order}

    tasks:
      place_order:
        # id_field adds the task id as order_id. timestamp comes from the record.
        payload: {customer_id: 42, total_amount: 59.97, status: pending}
        id_field: order_id
    ```

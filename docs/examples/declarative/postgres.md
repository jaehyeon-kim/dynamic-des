# Relational DB (Postgres) Declarative API

This example demonstrates how to integrate `dynamic-des` into a relational database architecture using the declarative **Standard API (`SimulationContext`)**.

By replacing the local console egress with `PostgresEgress`, the simulation becomes a fully detached data generator. It automatically streams complex, interrelated data (like parent `orders` and child `order_items`) directly into your PostgreSQL database using high-performance, asynchronous bulk inserts via `asyncpg`.

---

## 1. Multiplexing Multiple Tables

When generating complex relational data, a single event loop often produces records that belong in completely different SQL tables.

Instead of creating separate queues or complex routing logic, `PostgresEgress` supports **Table Multiplexing**. By attaching multiple instances of `PostgresEgress` to the simulation and tagging your output dictionaries with a special `__table__` key, the framework automatically filters and routes the records to their correct destinations.

```python
app = (
    SimulationContext(sim_id="Store", factor=1.0)
    .add_egress(PostgresEgress(DSN, table_name="orders"))
    .add_egress(PostgresEgress(DSN, table_name="order_items"))
)
```

## 2. Generating Interrelated Data

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

## 3. Dynamic Parameter Updates (Ingress)

This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation is running, you can dynamically update parameters (like speeding up the order arrival rate) simply by executing an `INSERT` statement in your database! The simulation will instantly fetch the new configuration.

## 4. Quick Start

Download the script, then run it. The run keeps generating orders until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the SQL command below.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/postgres_example.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Spin up the Postgres database
odctl up postgres

# 3. Run the declarative simulation
uv run --no-project --with "dynamic-des[postgres]" postgres_example.py
```

### With pip

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

---

## Full Source Code

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

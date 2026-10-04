# Relational DB (Postgres, YAML)

This example builds the same simulation as the [declarative Postgres example](../declarative/postgres.md) from a YAML blueprint. Two `PostgresEgress` instances are attached, one per table, and `PostgresIngress` polls `simulation_params` for updates. The order generator, which builds an order and its items with derived totals, stays in `postgres_logic.py` as a process.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/postgres.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/postgres_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Postgres database
odctl up postgres

# 3. Run the blueprint (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[postgres]" dynamic-des run postgres.yaml

# 4. Clean up the infrastructure when finished
odctl down postgres --volumes
```

### With pip

```bash
# 1. Install the package with the postgres extra, and odctl for the containers
pip install "dynamic-des[postgres]" "odctl>=0.5.1"

# 2. Start the Postgres database
odctl up postgres

# 3. Run the blueprint (Ctrl + C to stop)
dynamic-des run postgres.yaml

# 4. Clean up the infrastructure when finished
odctl down postgres --volumes
```

## What It Does

`run.before` calls `init_db`, which creates the `orders`, `order_items` and `simulation_params` tables and seeds the baseline rate. The run then writes orders and order items until you stop it.

In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
```

## Full Source Code

The order generator is listed under `processes`. It is a generator function that takes the context, the same function the Python example registers with `@app.arrival_loop`.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/postgres.yaml"
# Relational output with table multiplexing, in YAML.
#
# The twin of examples/declarative/postgres_example.py. Two PostgresEgress
# instances are attached, one per table, and each keeps only the records whose
# __table__ key matches its own table_name. PostgresIngress polls simulation_params
# for parameter updates. The order generator and the schema setup stay in
# postgres_logic.py beside this file.
#
# Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
# Run it with: dynamic-des run examples/yaml/postgres.yaml

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
      table_name: orders
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      table_name: order_items

arrivals:
  customer_order: {dist: exponential, rate: 1.0}

processes:
  # Builds an order and its items on every customer_order arrival.
  - !python postgres_logic.order_generator

run:
  before:
    - !python postgres_logic.init_db
```

```python title="examples/yaml/postgres_logic.py"
"""Python for examples/yaml/postgres.yaml: the order generator and the schema."""

import asyncio
import logging
import random
from datetime import datetime

import asyncpg

logger = logging.getLogger(__name__)

# Connection string matching the odctl `postgres` profile
DSN = "postgresql://user:password@localhost:5432/odctl"


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


async def _init_db():
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


def init_db():
    """Initializes the database schema before starting the simulation."""
    asyncio.run(_init_db())
    logger.info(
        "Test Ingress by running: INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
    )
```

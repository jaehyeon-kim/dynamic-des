# Orders with Line Items (Advanced YAML)

This example builds a simulation like the [declarative Postgres example](postgres.md) from a YAML blueprint with one Python function. Every customer order has 1 to 5 line items with random products, prices and quantities, and a total computed from them. A mapping payload is a constant, so the order generator is a process in `postgres_orders_logic.py`, referenced with `!python`. See [Advanced YAML: Custom Logic with `!python`](../guides/yaml-advanced.md) for how references work.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/advanced/postgres_orders.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/advanced/postgres_orders_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=1.0,<2"

# 2. Start the Postgres database
odctl up postgres

# 3. Run the blueprint (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[postgres]" ddes run postgres_orders.yaml

# 4. Clean up the infrastructure when finished
odctl down postgres --volumes
```

### With pip

```bash
# 1. Install the package with the postgres extra, and odctl for the containers
pip install "dynamic-des[postgres]" "odctl>=1.0,<2"

# 2. Start the Postgres database
odctl up postgres

# 3. Run the blueprint (Ctrl + C to stop)
ddes run postgres_orders.yaml

# 4. Clean up the infrastructure when finished
odctl down postgres --volumes
```

## What It Does

When the run starts, each `PostgresEgress` creates its table, `orders` or `order_items`, and `PostgresIngress` creates `simulation_params`, if they do not exist. The run then writes orders and their items until you stop it. Both egresses receive every record and each keeps the records whose `__table__` key names its table.

In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
```

## Full Source Code

The generator is listed under `processes` with `kwargs`, so it is called as `order_generator(context, arrival="customer_order", max_items=5)`. It draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a seeded run repeats the same orders.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/advanced/postgres_orders.yaml"
# Orders with random line items, in YAML with one Python function.
#
# Each customer_order arrival builds an order with 1 to 5 items, random prices and
# quantities, and a total computed from them. A mapping payload is a constant, so
# the generator is Python in postgres_orders_logic.py beside this file, referenced
# with !python. Everything else is plain YAML.
#
# Two PostgresEgress instances are attached, one per table, and each keeps only the
# records whose __table__ key names its table. Each creates its table at start.
#
# Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
# Run it with: ddes run examples/yaml/advanced/postgres_orders.yaml

simulation:
  sim_id: Store
  factor: 1.0
  random_seed: 42

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
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      tables:
        order_items:
          columns:
            order_item_id: INT
            order_id: INT
            product_id: INT
            quantity: INT
            unit_price: REAL
          primary_key: order_item_id

arrivals:
  # No spawn: the process below waits on this arrival itself.
  customer_order: {dist: exponential, rate: 1.0}

processes:
  # Called as order_generator(context, arrival="customer_order", max_items=5).
  - function: !python postgres_orders_logic.order_generator
    kwargs: {arrival: customer_order, max_items: 5}
```

```python title="examples/yaml/advanced/postgres_orders_logic.py"
"""Python for postgres_orders.yaml: an order generator with random line items."""


def order_generator(context, arrival: str, max_items: int):
    """Publishes an order and its items on every arrival.

    Draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a
    seeded run repeats the same orders. The `__table__` key names the table each
    record is written to.
    """
    rng = context.sampler.rng
    order_id = 1
    item_id = 1
    while True:
        yield context.wait_for_arrival(arrival)

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            context.env.publish_event(
                f"order-{order_id}",
                {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": int(rng.integers(1, 51)),
                    "quantity": quantity,
                    "unit_price": price,
                },
            )
            item_id += 1

        context.env.publish_event(
            f"order-{order_id}",
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1
```

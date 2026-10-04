# Relational DB (Postgres, YAML)

This example builds a simulation like the [declarative Postgres example](../declarative/postgres.md) from a YAML blueprint, with no Python and one table. Each customer order is written to `orders`, and `PostgresIngress` polls `simulation_params` for updates. The declarative example's orders with random line items are the [advanced Postgres example](advanced-postgres-orders.md), which keeps its generator in Python.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/postgres.yaml
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

When the run starts, `PostgresEgress` creates the `orders` table and `PostgresIngress` creates `simulation_params`, if they do not exist. The run then writes one order per arrival until you stop it. The order id is the task id, so a second run skips the orders the first one wrote, until the table is dropped.

In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
```

## Full Source Code

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

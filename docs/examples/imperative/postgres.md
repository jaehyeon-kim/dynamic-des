# Relational DB (Postgres) Imperative API

This example demonstrates how to integrate `dynamic-des` into a relational database architecture using the low-level **Imperative API**.

By replacing the local console egress with `PostgresEgress`, the simulation becomes a fully detached data generator. It automatically streams complex, interrelated data (like parent `orders` and child `order_items`) directly into your PostgreSQL database using high-performance, asynchronous bulk inserts via `asyncpg`.

---

## 1. Multiplexing Multiple Tables

When generating complex relational data, a single simulation process often produces records that belong in completely different SQL tables.

Instead of creating separate queues or complex routing logic, `PostgresEgress` supports **Table Multiplexing**. By attaching multiple instances of `PostgresEgress` to the `DynamicRealtimeEnvironment` and tagging your output dictionaries with a special `__table__` key, the framework automatically filters and routes the records to their correct destinations.

```python
egress_orders = PostgresEgress(DSN, table_name="orders")
egress_items = PostgresEgress(DSN, table_name="order_items")

env = DynamicRealtimeEnvironment(factor=1.0)
env.setup_egress([egress_orders, egress_items])
```

## 2. Generating Interrelated Data

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

## 3. Dynamic Parameter Updates (Ingress)

This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation is running, you can dynamically update parameters (like speeding up the order arrival rate) simply by executing an `INSERT` statement in your database! The simulation will instantly fetch the new configuration.

## 4. Quick Start

You can easily run this demo using the pre-configured CLI entry points. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the SQL command below.

```bash
# 1. Spin up the Postgres database via Docker Compose
uv run ddes-postgres-infra-up

# 2. Run the imperative simulation
uv run ddes-imperative-postgres
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the database container and inject the parameter (maintains audit history!)
docker exec -it postgres psql -U user -d ddes -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
```
*You will immediately see the simulation terminal log that the update was ingested and start generating orders much faster!*

```bash
# 3. Clean up the infrastructure when finished
uv run ddes-postgres-infra-down
```

---

## Full Source Code

This script connects the simulation to PostgreSQL, automatically initializes the database schema, and generates continuous streams of interrelated commerce data.

```python
--8<-- "src/dynamic_des/examples/imperative/postgres_example.py"
```

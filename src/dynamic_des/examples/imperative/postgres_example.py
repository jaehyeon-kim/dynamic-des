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

DSN = "postgresql://user:password@localhost:5432/ddes"

async def init_db():
    """Initializes the database schema before starting the simulation."""
    conn = await asyncpg.connect(DSN)
    await conn.execute('''
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
    ''')
    
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

    # Attach two egress instances, multiplexing across the shared output queue
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
                "status": "pending"
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
                    "unit_price": price
                }
                env.publish_event(f"item-{item_id}", item)
                item_id += 1
                
            order["total_amount"] = round(total, 2)
            env.publish_event(f"order-{order_id}", order)
            
            order_id += 1

    env.process(order_process(env))

    logger.info("Starting Imperative Postgres Demo. Press Ctrl+C to stop...")
    logger.info("Test Ingress by running: INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');")
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

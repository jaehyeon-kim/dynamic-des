import asyncio
import logging
import random
from datetime import datetime

import asyncpg

from dynamic_des import PostgresEgress, PostgresIngress, SimulationContext

logger = logging.getLogger(__name__)

# Connection string matching docker-compose
DSN = "postgresql://user:password@localhost:5432/ddes"

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
            "status": "pending"
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
                "unit_price": price
            }
            context.publish("order_event", item)
            item_id += 1
            
        order["total_amount"] = round(total, 2)
        context.publish("order_event", order)
        
        order_id += 1

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
    logger.info("Starting Declarative Postgres Demo. Press Ctrl+C to stop...")
    logger.info("Test Ingress by running: INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');")
    asyncio.run(init_db())
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

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

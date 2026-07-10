import asyncio
import logging
import queue

import pytest

from dynamic_des.connectors.egress.local import ConsoleEgress
from dynamic_des.connectors.ingress.local import LocalIngress


@pytest.mark.asyncio
async def test_local_ingress():
    """Verify LocalIngress accurately schedules and queues updates."""
    ingress_queue = queue.Queue()
    schedule = [(0.01, "Line_A.lathe.capacity", 5), (0.02, "Line_A.lathe.capacity", 10)]
    ingress = LocalIngress(schedule)

    await ingress.run(ingress_queue)

    assert ingress_queue.qsize() == 2
    assert ingress_queue.get_nowait() == ("Line_A.lathe.capacity", 5)
    assert ingress_queue.get_nowait() == ("Line_A.lathe.capacity", 10)


@pytest.mark.asyncio
async def test_console_egress(caplog):
    """Verify ConsoleEgress pops from queue and formats to the logger."""
    caplog.set_level(logging.INFO)
    egress_queue = queue.Queue()
    egress = ConsoleEgress()

    batch = [
        {"stream_type": "telemetry", "path_id": "test", "value": 1},
        {"stream_type": "event", "key": "task-1", "value": {"status": "ok"}},
    ]
    egress_queue.put(batch)

    task = asyncio.create_task(egress.run(egress_queue))
    await asyncio.sleep(0.05)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    assert "[TEL] {'path_id': 'test', 'value': 1}" in caplog.text
    assert "[EVT] {'key': 'task-1', 'value': {'status': 'ok'}}" in caplog.text

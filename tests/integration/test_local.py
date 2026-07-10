import asyncio
import queue
import pytest
from dynamic_des.connectors.egress.local import ConsoleEgress

import logging


@pytest.mark.asyncio
@pytest.mark.integration
async def test_console_egress(caplog):
    """
    Tests that the ConsoleEgress effectively logs to stdout.
    This acts as a basic 'local' integration test without external infrastructure.
    """
    egress = ConsoleEgress()
    q = queue.Queue()

    mock_event = [
        {"key": "order-1", "value": {"amount": 100}},
        {"path_id": "system.cpu", "value": 42},
    ]

    with caplog.at_level(logging.INFO):
        q.put(mock_event)

        task = asyncio.create_task(egress.run(q))
        await asyncio.sleep(0.5)
        task.cancel()

        assert "order-1" in caplog.text
        assert "system.cpu" in caplog.text

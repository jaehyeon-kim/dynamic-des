import asyncio
import json
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynamic_des.connectors.egress.redis import RedisEgress


@pytest.mark.asyncio
async def test_redis_egress():
    egress_queue = queue.Queue()

    # Put two batches
    egress_queue.put(
        [
            {"stream_type": "event", "key": "e1", "value": "v1"},
            {"stream_type": "telemetry", "key": "t1", "value": "v2"},
        ]
    )
    egress_queue.put([{"__stream__": "custom_stream", "key": "c1"}])

    egress = RedisEgress(url="redis://localhost:6379/0", stream_name="default_stream")

    with patch("dynamic_des.connectors.egress.redis.redis.from_url") as mock_from_url:
        from unittest.mock import MagicMock

        mock_client = AsyncMock()
        mock_pipe = AsyncMock()
        mock_client.pipeline = MagicMock(return_value=mock_pipe)
        mock_from_url.return_value = mock_client

        task = asyncio.create_task(egress.run(egress_queue))
        await asyncio.sleep(0.1)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_from_url.assert_called_once_with("redis://localhost:6379/0")
        assert mock_pipe.xadd.call_count == 3
        # Check first call (default stream)
        args, _ = mock_pipe.xadd.call_args_list[0]
        assert args[0] == "default_stream"
        assert "payload" in args[1]

        # Check third call (custom stream)
        args, _ = mock_pipe.xadd.call_args_list[2]
        assert args[0] == "custom_stream"


@pytest.mark.asyncio
async def test_stream_key_is_read_from_the_nested_payload():
    """`publish_event` nests the caller's dict under `value`, so `__stream__` is there
    rather than at the top level. Reading only the top level sent every record to the
    default stream and the documented routing silently did nothing."""
    egress = RedisEgress("redis://localhost:6379", stream_name="default_events")
    egress.client = AsyncMock()
    pipe = AsyncMock()
    egress.client.pipeline = MagicMock(return_value=pipe)

    q: queue.Queue = queue.Queue()
    q.put(
        [
            {
                "stream_type": "event",
                "key": "part-1",
                "value": {"__stream__": "part_events", "status": "queued"},
            },
            {"stream_type": "telemetry", "value": {"metric": 1}},
        ]
    )

    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(0.2)
    task.cancel()

    targets = [call.args[0] for call in pipe.xadd.call_args_list]
    assert targets == ["part_events", "default_events"]

    routed = json.loads(pipe.xadd.call_args_list[0].args[1]["payload"])
    assert "__stream__" not in routed["value"], "the key should not reach the stream"
    assert routed["value"] == {"status": "queued"}

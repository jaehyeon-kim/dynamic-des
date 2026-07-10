import asyncio
import queue
from unittest.mock import AsyncMock, patch

import pytest

from dynamic_des.connectors.egress.redis import RedisEgress


@pytest.mark.asyncio
async def test_redis_egress():
    egress_queue = queue.Queue()
    
    # Put two batches
    egress_queue.put([
        {"stream_type": "event", "key": "e1", "value": "v1"},
        {"stream_type": "telemetry", "key": "t1", "value": "v2"}
    ])
    egress_queue.put([
        {"__stream__": "custom_stream", "key": "c1"}
    ])
    
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

import asyncio
import queue
from unittest.mock import AsyncMock, patch

import orjson
import pytest

from dynamic_des.connectors.ingress.redis import RedisIngress


@pytest.mark.asyncio
async def test_redis_ingress():
    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", channel_name="test_params", poll_interval=0.01)
    
    with patch("dynamic_des.connectors.ingress.redis.redis.from_url") as mock_from_url:
        from unittest.mock import MagicMock
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)
        mock_from_url.return_value = mock_client
        
        # Mock get_message to return a valid message, then None to sleep
        mock_pubsub.get_message.side_effect = [
            {
                "type": "message",
                "data": orjson.dumps({"param_path": "a.b", "param_value": 42})
            },
            None,  # Sleep
            asyncio.CancelledError() # Break out of loop
        ]
        
        task = asyncio.create_task(ingress.run(update_queue))
        
        try:
            await task
        except asyncio.CancelledError:
            pass
            
        mock_from_url.assert_called_once_with("redis://localhost:6379/0")
        mock_pubsub.subscribe.assert_called_once_with("test_params")
        
        assert not update_queue.empty()
        path, val = update_queue.get_nowait()
        assert path == "a.b"
        assert val == 42
        
        mock_pubsub.unsubscribe.assert_called_once_with("test_params")
        mock_client.close.assert_called_once()

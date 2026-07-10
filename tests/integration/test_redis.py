import asyncio
import queue
import pytest
import redis.asyncio as redis
import orjson

from dynamic_des.connectors.egress.redis import RedisEgress
from dynamic_des.connectors.ingress.redis import RedisIngress

@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_integration(redis_container):
    url = redis_container
    
    # 1. Test Egress
    egress_queue = queue.Queue()
    egress = RedisEgress(url=url, stream_name="test_stream")
    egress_task = asyncio.create_task(egress.run(egress_queue))
    
    test_data = [{"__stream__": "test_stream", "event_id": 1, "value": "test"}]
    egress_queue.put(test_data)
    
    # Wait a bit for egress to process
    await asyncio.sleep(2)
    
    # Verify via direct client
    client = redis.from_url(url)
    streams = await client.xread({"test_stream": "0-0"})
    assert len(streams) > 0
    assert len(streams[0][1]) == 1
    
    payload = streams[0][1][0][1][b"payload"]
    parsed = orjson.loads(payload)
    assert parsed["event_id"] == 1
    
    egress_task.cancel()
    try:
        await egress_task
    except asyncio.CancelledError:
        pass
        
    # 2. Test Ingress
    update_queue = queue.Queue()
    ingress = RedisIngress(url=url, channel_name="test_params", poll_interval=0.1)
    ingress_task = asyncio.create_task(ingress.run(update_queue))
    
    # Wait for subscription
    await asyncio.sleep(1)
    
    # Publish data
    publish_data = {"param_path": "test.path", "param_value": 42}
    await client.publish("test_params", orjson.dumps(publish_data))
    
    # Wait for ingress to process
    await asyncio.sleep(1)
    
    assert not update_queue.empty()
    path, value = update_queue.get_nowait()
    assert path == "test.path"
    assert value == 42
    
    ingress_task.cancel()
    try:
        await ingress_task
    except asyncio.CancelledError:
        pass
        
    await client.close()

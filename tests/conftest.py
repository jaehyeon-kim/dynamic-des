import asyncio
import queue

import pytest

from dynamic_des.core.environment import DynamicRealtimeEnvironment
from dynamic_des.models.params import CapacityConfig, DistributionConfig, SimParameter


@pytest.fixture
def run_until():
    """
    Runs a coroutine as a task until `done()` is true, then cancels it.

    Egress tests used to cancel after a fixed sleep, which a slow CI runner can
    outlast before the first write. Waiting on the result itself, with a time limit,
    removes that race; the test's own assertions then report what went wrong.
    """

    async def _run(coro, done, timeout=5.0):
        task = asyncio.create_task(coro)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not done() and loop.time() < deadline:
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    return _run


@pytest.fixture
def env():
    """Provides a fresh standard SimPy environment for each test."""
    return DynamicRealtimeEnvironment(factor=0.0)


@pytest.fixture
def registry(env):
    """Provides a SimulationRegistry attached to the test environment."""
    return env.registry


@pytest.fixture
def sample_params():
    """Provides a standard 'Line_A' SimParameter set for testing."""
    return SimParameter(
        sim_id="Line_A",
        arrival={
            "standard": DistributionConfig(dist="exponential", rate=1 / 10.0),
            "priority": DistributionConfig(dist="exponential", rate=1 / 50.0),
        },
        service={
            "setup": DistributionConfig(dist="normal", mean=2.0, std=0.5),
            "milling": DistributionConfig(dist="normal", mean=5.0, std=1.2),
        },
        resources={
            "lathe": CapacityConfig(current_cap=2, max_cap=5),
            "operator": CapacityConfig(current_cap=1, max_cap=3),
        },
        containers={
            "tank1": CapacityConfig(current_cap=50.0, max_cap=100.0),
        },
        stores={
            "buffer1": CapacityConfig(current_cap=5, max_cap=10),
        },
    )


@pytest.fixture
def mock_ingress_queue():
    """
    Async Testing Fixtures

    Provides a thread-safe queue to simulate data arriving from Kafka/Redis.
    This is required for aiokafka and other async providers.
    """
    return queue.Queue()


@pytest.fixture
def mock_egress_queue():
    """
    Async Testing Fixtures

    Provides a thread-safe queue to simulate data stresming to Kafka/Redis.
    This is required for aiokafka and other async providers.
    """
    return queue.Queue()

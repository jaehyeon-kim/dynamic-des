import pytest
from dynamic_des.core.registry import SimulationRegistry
from dynamic_des.models.params import DistributionConfig, ResourceConfig, SimParameter
from simpy import Environment


@pytest.fixture
def env():
    """Provides a fresh standard SimPy environment for each test."""
    return Environment()


@pytest.fixture
def registry(env):
    """Provides a SimulationRegistry attached to the test environment."""
    return SimulationRegistry(env)


@pytest.fixture
def sample_params():
    """Provides a standard 'Line_A' SimParameter set for testing."""
    return SimParameter(
        param_id="Line_A",
        arrival=DistributionConfig(dist="exponential", rate=0.1),
        service={"milling": DistributionConfig(dist="normal", mean=5.0, std=1.0)},
        resources={"lathe": ResourceConfig(current_cap=1, max_cap=2)},
    )


@pytest.fixture
def mock_bridge_queue():
    """
    Async Testing Fixtures

    Provides a thread-safe queue to simulate data arriving from Kafka/Redis.
    This is required for aiokafka and other async providers.
    """
    import queue

    return queue.Queue()

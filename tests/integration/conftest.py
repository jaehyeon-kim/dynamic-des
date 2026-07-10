import pytest
import asyncio
from pathlib import Path
from python_on_whales import DockerClient


@pytest.fixture(scope="session")
def check_docker():
    """Verify Docker is available, skip tests if not."""
    try:
        from python_on_whales import docker

        docker.version()
    except Exception:
        pytest.skip("Docker is not available. Skipping integration tests.")


@pytest.fixture(scope="module")
def postgres_container(check_docker):
    """
    Spins up the postgres container via python-on-whales for integration testing.
    """
    # Start the container using the existing project docker-compose
    compose_file = (
        Path(__file__).parent.parent.parent
        / "src"
        / "dynamic_des"
        / "examples"
        / "docker-compose.yml"
    )
    client = DockerClient(
        compose_files=[str(compose_file)], compose_profiles=["postgres"]
    )
    client.compose.up(detach=True)

    dsn = "postgresql://user:password@localhost:5432/ddes"
    import asyncpg
    import time

    # Wait for Postgres to be ready
    for _ in range(30):
        try:

            async def check():
                conn = await asyncpg.connect(dsn)
                await conn.close()

            asyncio.run(check())
            break
        except Exception:
            time.sleep(1)
    else:
        client.compose.down(volumes=True)
        pytest.fail("Postgres did not start in time.")

    yield dsn

    # Tear down
    client.compose.down(volumes=True)


@pytest.fixture(scope="module")
def kafka_container(check_docker):
    """
    Spins up the kafka broker container via python-on-whales for integration testing.
    """
    compose_file = (
        Path(__file__).parent.parent.parent
        / "src"
        / "dynamic_des"
        / "examples"
        / "docker-compose.yml"
    )
    client = DockerClient(compose_files=[str(compose_file)], compose_profiles=["kafka"])
    client.compose.up(detach=True)

    bootstrap_servers = "localhost:9092"
    import time

    # Simple wait to ensure broker is up (can use aiokafka to be more strict if desired)
    time.sleep(10)

    yield bootstrap_servers

    # Tear down
    client.compose.down(volumes=True)

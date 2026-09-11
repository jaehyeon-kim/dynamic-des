import asyncio
import shutil
import subprocess
import time
import urllib.request

import pytest

# Example infrastructure comes from odctl (https://github.com/jaehyeon-kim/odctl).
# Install it with `uv tool install "odctl>=0.5.1"` or `pip install "odctl>=0.5.1"`.
# 0.5.1 is the floor: earlier versions created the Valkey user without a channel
# grant, so RedisIngress could not subscribe (odctl#76).
ODCTL = "odctl"


def _run_odctl(*args: str) -> None:
    """Run an odctl command and raise with its output when it fails."""
    subprocess.run([ODCTL, *args], check=True, capture_output=True, text=True)


@pytest.fixture(scope="session")
def check_docker():
    """Verify Docker and odctl are available, skip integration tests if not."""
    if shutil.which(ODCTL) is None:
        pytest.skip("odctl is not installed. Skipping integration tests.")
    try:
        subprocess.run(
            ["docker", "version"], check=True, capture_output=True, text=True
        )
    except Exception:
        pytest.skip("Docker is not available. Skipping integration tests.")


@pytest.fixture(scope="session")
def odctl_profile(check_docker):
    """
    Bring odctl profiles up on demand and tear down every started profile at the
    end of the session.

    A profile is started once per session. Restarting it between modules costs a
    full image pull and health-check cycle for no benefit, because each test
    creates the topics and tables it needs.
    """
    started: list[str] = []

    def _up(profile: str) -> None:
        if profile not in started:
            _run_odctl("up", profile)
            started.append(profile)

    yield _up

    for profile in reversed(started):
        _run_odctl("down", profile, "--volumes")


@pytest.fixture(scope="session")
def postgres_container(odctl_profile):
    """Starts the odctl `postgres` profile and yields its DSN."""
    odctl_profile("postgres")

    # odctl names the default database `odctl`, with the user/password pair
    # reported by `odctl explain postgres`.
    dsn = "postgresql://user:password@localhost:5432/odctl"
    import asyncpg

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
        pytest.fail("Postgres did not start in time.")

    yield dsn


@pytest.fixture(scope="session")
def kafka_container(odctl_profile):
    """Starts the odctl `kafka-lite` profile and yields its bootstrap servers."""
    odctl_profile("kafka-lite")

    bootstrap_servers = "localhost:9092"

    # odctl waits for the broker health check before returning, so the listener
    # is already accepting connections here.
    yield bootstrap_servers


@pytest.fixture(scope="session")
def schema_registry(kafka_container):
    """Yields the Karapace URL from the odctl `kafka-lite` profile.

    `kafka-lite` carries Karapace on 8081 next to the broker, so no second profile
    is needed. odctl returns once the broker health check passes, which says nothing
    about Karapace, so poll the registry itself before handing the URL over.

    Address it as 127.0.0.1 rather than localhost. Karapace binds IPv4 only, while
    localhost resolves to ::1 first on macOS, so every request is reset before it
    reaches the container.
    """
    url = "http://127.0.0.1:8081"

    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{url}/subjects", timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Karapace did not start in time.")

    yield url


@pytest.fixture(scope="session")
def redis_container(odctl_profile):
    """Starts the odctl `valkey` profile and yields its connection URL."""
    odctl_profile("valkey")

    # odctl disables the unauthenticated default Valkey user, so the URL has to
    # carry the `user` / `password` pair.
    yield "redis://user:password@localhost:6379/0"

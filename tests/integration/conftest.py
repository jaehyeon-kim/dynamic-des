import asyncio
import shutil
import subprocess
import time

import pytest

# Example infrastructure comes from odctl (https://github.com/jaehyeon-kim/odctl).
# Install it with `uv tool install odctl` or `pip install odctl`.
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
def redis_container(odctl_profile):
    """Starts the odctl `valkey` profile and yields its connection URL."""
    odctl_profile("valkey")

    # odctl creates the `user` account with `~* +@all`, which covers keys and
    # commands but not Pub/Sub channels. Valkey defaults new users to
    # `resetchannels`, so SUBSCRIBE and PUBLISH are refused with NOPERM until the
    # account is granted `allchannels`. RedisEgress writes to streams and works
    # without this; RedisIngress subscribes and does not.
    subprocess.run(
        [
            "docker",
            "exec",
            "valkey",
            "valkey-cli",
            "--user",
            "user",
            "--pass",
            "password",
            "ACL",
            "SETUSER",
            "user",
            "allchannels",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    # odctl disables the unauthenticated default Valkey user, so the URL has to
    # carry the `user` / `password` pair.
    yield "redis://user:password@localhost:6379/0"

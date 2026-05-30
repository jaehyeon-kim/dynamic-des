import logging
import sys
from pathlib import Path

from python_on_whales import DockerClient

# Configuration & Pathing
EXAMPLE_DIR = Path(__file__).parent
DOCKER_COMPOSE_FILE = EXAMPLE_DIR / "docker-compose.yml"

# Initialize a logger specifically for the examples orchestration
logger = logging.getLogger(__name__)


# Centralized Logging Architecture
def setup_example_logging(level: int = logging.INFO):
    """
    Configures a unified logging format for the entire 'dynamic_des' namespace.
    This guarantees that the CLI, infrastructure scripts, and simulation demos
    all output consistent, readable logs to the user's terminal.
    """
    # Target the top-level namespace of the library
    library_logger = logging.getLogger("dynamic_des")

    # Specifically check for a StreamHandler (ignoring the core NullHandler)
    has_console_handler = any(
        isinstance(h, logging.StreamHandler) for h in library_logger.handlers
    )

    # Prevent duplicate logs if the function is called multiple times
    if not has_console_handler:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"
        )
        handler.setFormatter(formatter)

        library_logger.addHandler(handler)
        library_logger.setLevel(level)

        # Stop logs from bubbling up to the root logger (prevents double-printing
        # if the user has their own logging configured in a parent app)
        library_logger.propagate = False


# Infrastructure Lifecycle (IaC)
def manage_infrastructure(profile: str, down: bool = False):
    """
    Orchestrates the infrastructure lifecycle using Docker Compose profiles.
    """
    # Ensure our CLI logger is ready
    setup_example_logging()

    # Bind the Docker client to our specific compose file AND the requested profile
    docker = DockerClient(
        compose_files=[str(DOCKER_COMPOSE_FILE)], compose_profiles=[profile]
    )

    if down:
        logger.info(f"Initiating teardown: Stopping '{profile}' infrastructure...")
        try:
            # volumes=True mimics the `docker compose down -v` command
            docker.compose.down(volumes=True)
            logger.info(f"'{profile}' infrastructure cleanly stopped and removed.")
        except Exception as e:
            logger.error(f"Failed to stop infrastructure: {e}")
            sys.exit(1)
    else:
        logger.info(f"Bootstrapping '{profile}' via {DOCKER_COMPOSE_FILE.name}...")
        try:
            docker.compose.up(detach=True)
            logger.info(f"'{profile}' infrastructure is up and running.")
        except Exception as e:
            logger.critical(f"Docker orchestration failed: {e}")
            logger.critical(
                "Troubleshooting: Is Docker Desktop running? Is the YAML valid?"
            )
            sys.exit(1)


# ==========================================
# CLI Entry Points: Declarative (Context API)
# ==========================================
def declarative_local_demo():
    setup_example_logging()
    from .declarative.local_example import run

    run()


def declarative_history_demo(auto_down: bool = False):
    setup_example_logging()
    from .declarative.history_example import run

    try:
        run()
    finally:
        if auto_down:
            manage_infrastructure("storage", down=True)


def declarative_kafka_demo(auto_down: bool = False):
    setup_example_logging()
    from .declarative.kafka_example import run

    try:
        run()
    finally:
        if auto_down:
            manage_infrastructure("kafka", down=True)


# ==========================================
# CLI Entry Points: Imperative (Env API)
# ==========================================
def imperative_local_demo():
    """CLI entry point: Runs the local-only simulation demo."""
    setup_example_logging()
    logger.info("Starting local-only simulation...")
    from .imperative.local_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def imperative_kafka_demo(auto_down: bool = False):
    """CLI entry point: Runs the Kafka-integrated simulation demo."""
    setup_example_logging()
    logger.info("Starting Kafka-integrated simulation...")
    from .imperative.kafka_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")
    finally:
        if auto_down:
            logger.info("Auto-teardown enabled. Cleaning up Kafka infrastructure...")
            manage_infrastructure(profile="kafka", down=True)


def imperative_history_demo(auto_down: bool = False):
    """CLI entry point: Runs the historical batch generation demo."""
    setup_example_logging()
    logger.info("Starting historical data generation to S3/Parquet...")
    from .imperative.history_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")
    finally:
        if auto_down:
            logger.info("Auto-teardown enabled. Cleaning up storage infrastructure...")
            manage_infrastructure(profile="storage", down=True)


def kafka_dashboard_demo():
    """CLI entry point: Runs the real-time NiceGUI dashboard."""
    setup_example_logging()
    logger.info("Launching real-time dashboard on http://localhost:8080")
    from .kafka_dashboard import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("Dashboard shutdown requested.")


# Infrastructure Wrappers
def kafka_infra_up():
    """Starts the Kafka Docker containers in the background."""
    manage_infrastructure(profile="kafka", down=False)


def kafka_infra_down():
    """Stops and removes the Kafka Docker containers."""
    manage_infrastructure(profile="kafka", down=True)


def storage_infra_up():
    """Starts the SeaweedFS/S3 Docker containers in the background."""
    manage_infrastructure(profile="storage", down=False)


def storage_infra_down():
    """Stops and removes the SeaweedFS/S3 Docker containers."""
    manage_infrastructure(profile="storage", down=True)

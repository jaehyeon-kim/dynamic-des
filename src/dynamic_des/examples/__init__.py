import logging
import sys
from pathlib import Path

from python_on_whales import DockerClient

# Configuration & Pathing
EXAMPLE_DIR = Path(__file__).parent
KAFKA_COMPOSE = EXAMPLE_DIR / "compose-kafka.yml"

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

    # Prevent duplicate logs if the function is called multiple times
    if not library_logger.handlers:
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
def kafka_cluster(down: bool = False):
    """
    Orchestrates the Kafka infrastructure lifecycle using Docker Compose.
    """
    # Ensure our CLI logger is ready
    setup_example_logging()

    # Bind the Docker client to our specific compose file
    docker = DockerClient(compose_files=[str(KAFKA_COMPOSE)])

    if down:
        logger.info("Initiating teardown: Stopping Kafka infrastructure...")
        try:
            docker.compose.down()
            logger.info("Kafka infrastructure cleanly stopped and removed.")
        except Exception as e:
            logger.error(f"Failed to stop infrastructure: {e}")
            sys.exit(1)
    else:
        logger.info(f"Bootstrapping Kafka via {KAFKA_COMPOSE.name}...")
        try:
            docker.compose.up(detach=True)
            logger.info("Infrastructure is up. Broker reachable at localhost:9092")
        except Exception as e:
            logger.critical(f"Docker orchestration failed: {e}")
            logger.critical(
                "Troubleshooting: Is Docker Desktop running? Is the YAML valid?"
            )
            sys.exit(1)


# CLI Entry Points
def local_demo():
    """CLI entry point: Runs the local-only simulation demo."""
    setup_example_logging()
    logger.info("Starting local-only simulation...")
    from .local_example import run

    run()


def kafka_demo(auto_down: bool = False):
    """CLI entry point: Runs the Kafka-integrated simulation demo."""
    setup_example_logging()
    logger.info("Starting Kafka-integrated simulation...")
    from .kafka_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")
    finally:
        if auto_down:
            logger.info("Auto-teardown enabled. Cleaning up infrastructure...")
            kafka_cluster(down=True)


def kafka_dashboard_demo():
    """CLI entry point: Runs the real-time NiceGUI dashboard."""
    setup_example_logging()
    logger.info("Launching real-time dashboard on http://localhost:8080")
    from .kafka_dashboard import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("Dashboard shutdown requested.")


def kafka_infra_up():
    """Starts the Kafka Docker containers in the background."""
    kafka_cluster(down=False)


def kafka_infra_down():
    """Stops and removes the Kafka Docker containers."""
    kafka_cluster(down=True)

import logging
import sys

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


# ==========================================
# CLI Entry Points: Declarative (Context API)
# ==========================================
def declarative_local_demo():
    setup_example_logging()
    from .declarative.local_example import run

    run()


def declarative_history_demo():
    setup_example_logging()
    from .declarative.history_example import run

    run()


def declarative_postgres_demo():
    setup_example_logging()
    from .declarative.postgres_example import run

    run()


def declarative_kafka_demo():
    setup_example_logging()
    from .declarative.kafka_example import run

    run()


def declarative_backfill_live_demo():
    setup_example_logging()
    from .declarative.backfill_live_example import run

    # This demo runs live after the backfill, so Ctrl-C is the normal way to end it.
    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def declarative_redis_demo():
    setup_example_logging()
    from .declarative.redis_example import run

    run()


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


def imperative_kafka_demo():
    """CLI entry point: Runs the Kafka-integrated simulation demo."""
    setup_example_logging()
    logger.info("Starting Kafka-integrated simulation...")
    from .imperative.kafka_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def imperative_postgres_demo():
    """CLI entry point: Runs the Postgres-integrated simulation demo."""
    setup_example_logging()
    logger.info("Starting Postgres-integrated simulation...")
    from .imperative.postgres_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def imperative_redis_demo():
    """CLI entry point: Runs the Redis-integrated simulation demo."""
    setup_example_logging()
    logger.info("Starting Redis-integrated simulation...")
    from .imperative.redis_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def imperative_history_demo():
    """CLI entry point: Runs the historical batch generation demo."""
    setup_example_logging()
    logger.info("Starting historical data generation to S3/Parquet...")
    from .imperative.history_example import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("User gracefully interrupted the simulation.")


def kafka_dashboard_demo():
    """CLI entry point: Runs the real-time NiceGUI dashboard."""
    setup_example_logging()
    logger.info("Launching real-time dashboard on http://localhost:8080")
    from .kafka_dashboard import run

    try:
        run()
    except KeyboardInterrupt:
        logger.info("Dashboard shutdown requested.")

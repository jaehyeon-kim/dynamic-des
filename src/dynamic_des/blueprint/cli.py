"""The `ddes` command."""

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from dynamic_des.blueprint.loader import BlueprintError
from dynamic_des.core.context import SimulationContext
from dynamic_des.utils import time_to_seconds

app = typer.Typer(
    help="Run dynamic-des simulations defined in YAML blueprint files.",
    no_args_is_help=True,
    add_completion=False,
)

logger = logging.getLogger("dynamic_des.cli")


def _show_version(value: bool) -> None:
    if value:
        from dynamic_des import __version__

        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Print the installed version and exit.",
            callback=_show_version,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Run dynamic-des simulations defined in YAML blueprint files."""


def _parse_until(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return time_to_seconds(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


@app.command()
def run(
    file: Annotated[
        Path,
        typer.Argument(
            help="The blueprint file.",
            metavar="FILE",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    until: Annotated[
        Optional[str],
        typer.Option(
            metavar="TIME",
            help="Simulation time to stop at, in seconds or as a duration such as "
            "'10 min'. Overrides run.until in the file.",
        ),
    ] = None,
) -> None:
    """Build the simulation in FILE and run it."""
    stop_at = _parse_until(until)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        context = SimulationContext.from_yaml(file)
    except BlueprintError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    try:
        context.run(until=stop_at)
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user.")

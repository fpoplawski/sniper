from __future__ import annotations

import logging
import time
from typing import Optional

import click

logger = logging.getLogger(__name__)

from .daily_runner import cfg, fetcher, run_once
from .db import migrate, DB_FILE
from . import aggregator, daily_report


@click.group()
def cli() -> None:
    """Command line interface."""


@cli.command()
@click.option("--once", is_flag=True, help="Run a single iteration and exit")
@click.option("--date", help="Departure date (YYYY-MM-DD) for manual tests")
def run(once: bool, date: Optional[str]) -> None:
    """Fetch new offers and process them."""
    migrate(db_path=DB_FILE)
    if once:
        run_once(date)
    else:
        while True:
            run_once(date)
            time.sleep(cfg.poll_interval_h * 3600)


@cli.command()
@click.option("--date", help="Departure date (YYYY-MM-DD) for manual tests")
def fetch(date: Optional[str]) -> None:
    """Fetch offers only and print them."""
    migrate(db_path=DB_FILE)
    for origin in cfg.origins or []:
        for dest in cfg.destinations or []:
            logger.info("Fetching: %s \u2794 %s", origin, dest)
            try:
                offers = fetcher.search_prices(
                    origin,
                    dest,
                    departure_at=date,
                    return_at=None,
                    currency=cfg.currency,
                )
            except Exception as exc:
                logger.warning(
                    "  Failed to fetch %s->%s: %s", origin, dest, exc
                )
                continue
            for off in offers:
                click.echo(off)


@cli.command()
def report() -> None:
    """Aggregate history and send daily report."""
    migrate(db_path=DB_FILE)
    aggregator.aggregate()
    daily_report.send_daily_report()


if __name__ == "__main__":
    cli()

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional
import time

import click

from .aviasales_fetcher import AviasalesFetcher
from .config import Config
from .steal_engine import is_steal
from .pair_engine import process_outbound
from .notifier import send_telegram
from .db import (
    insert_offer,
    mark_alert_sent,
    get_last_30d_avg,
    DB_FILE,
    migrate,
)

# ────────────────────────────────────────────────────────────────
# Konfiguracja
# ────────────────────────────────────────────────────────────────

cfg = Config.from_json()
fetcher = AviasalesFetcher(cfg.tp_token, cfg.tp_marker)

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("sniper.log"), logging.StreamHandler()],
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# Helper
# ────────────────────────────────────────────────────────────────


def travel_days(dep: date | None, ret: date | None) -> int:
    if not dep or not ret:
        return 0
    return (ret - dep).days


# ────────────────────────────────────────────────────────────────
# Główna logika
# ────────────────────────────────────────────────────────────────


def run_once(dep_date: Optional[str] = None) -> None:
    total_inserted: List[int] = []

    for origin in cfg.origins or []:
        for dest in cfg.destinations or []:
            logger.info("Fetching: %s ➔ %s", origin, dest)
            try:
                offers_iter = fetcher.search_prices(
                    origin,
                    dest,
                    departure_at=dep_date,
                    return_at=None,
                    currency=cfg.currency,
                )
            except Exception as exc:
                logger.warning(
                    "  Failed to fetch %s->%s: %s", origin, dest, exc
                )
                continue

            for off in offers_iter:
                # ── Filtry wstępne ────────────────────────────
                if off.stops > cfg.max_stops:
                    continue

                if off.max_layover_h and off.max_layover_h > cfg.max_layover_h:
                    continue

                days = travel_days(off.depart_date, off.return_date)
                if days and (
                    days < cfg.min_trip_days or days > cfg.max_trip_days
                ):
                    continue

                # ── Zapis do bazy ────────────────────────────
                offer_id = insert_offer(off, db_path=DB_FILE)
                total_inserted.append(offer_id)

                # ── Parowanie OW ────────────────────────────
                pair_steals = process_outbound(off, offer_id)
                if pair_steals:
                    logger.info("Utworzono %d STEAL par", len(pair_steals))

                # ── STEAL? ───────────────────────────────────
                if is_steal(off, cfg):
                    avg = (
                        get_last_30d_avg(off.origin, off.destination)
                        or off.price_pln
                    )
                    diff_pct = int(100 * (1 - off.price_pln / Decimal(avg)))
                    msg = (
                        f"✈️ STEAL!\n"
                        f"{off.origin} ➔ {off.destination}\n"
                        f"{off.depart_date} – {off.return_date or 'OW'}\n"
                        f"{off.price_pln} PLN — -{diff_pct}% "
                        f"vs średnia 30 dni\n"
                        f"[Rezerwuj]({off.deep_link})"
                    )
                    send_telegram(msg)
                    mark_alert_sent(offer_id, db_path=DB_FILE)

    logger.info("Inserted %s offers into DB", len(total_inserted))


def main() -> None:
    try:
        run_once()
    except Exception:
        logger.exception("daily_runner failed")


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
            logger.info("Fetching: %s ➔ %s", origin, dest)
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
    from . import aggregator, daily_report

    aggregator.aggregate()
    daily_report.send_daily_report()


if __name__ == "__main__":
    cli()

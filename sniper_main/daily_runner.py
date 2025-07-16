from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List

from .aviasales_fetcher import AviasalesFetcher
from .config import Config
from .steal_engine import is_steal
from .notifier import send_telegram
from .db import (
    insert_offer,
    mark_alert_sent,
    get_last_30d_avg,
    DB_FILE,
)

# ────────────────────────────────────────────────────────────────
# Konfiguracja
# ────────────────────────────────────────────────────────────────

cfg = Config.from_json()
fetcher = AviasalesFetcher(cfg.tp_token, cfg.tp_marker)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

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


def run_once() -> None:
    total_inserted: List[int] = []

    for origin in cfg.origins or []:
        for dest in cfg.destinations or []:
            logging.info("Fetching: %s ➔ %s", origin, dest)
            try:
                offers_iter = fetcher.search_prices(
                    origin,
                    dest,
                    departure_at=None,
                    return_at=None,
                    currency=cfg.currency,
                )
            except Exception as exc:
                logging.warning("  Failed to fetch %s->%s: %s", origin, dest, exc)
                continue

            for off in offers_iter:
                # ── Filtry wstępne ────────────────────────────
                if off.stops > cfg.max_stops:
                    continue

                if (
                    off.max_layover_h
                    and off.max_layover_h > cfg.max_layover_h
                ):
                    continue

                days = travel_days(off.depart_date, off.return_date)
                if days and (
                    days < cfg.min_trip_days or days > cfg.max_trip_days
                ):
                    continue

                # ── Zapis do bazy ────────────────────────────
                offer_id = insert_offer(off, db_path=DB_FILE)
                total_inserted.append(offer_id)

                # ── STEAL? ───────────────────────────────────
                if is_steal(off, cfg):
                    avg = get_last_30d_avg(off.origin, off.destination) or off.price_pln
                    diff_pct = int(100 * (1 - off.price_pln / Decimal(avg)))
                    msg = (
                        f"✈️ STEAL!\n"
                        f"{off.origin} ➔ {off.destination}\n"
                        f"{off.depart_date} – {off.return_date or 'OW'}\n"
                        f"{off.price_pln} PLN — -{diff_pct}% vs średnia 30 dni\n"
                        f"[Rezerwuj]({off.deep_link})"
                    )
                    send_telegram(msg)
                    mark_alert_sent(offer_id, db_path=DB_FILE)

    logging.info("Inserted %s offers into DB", len(total_inserted))


def main() -> None:
    try:
        run_once()
    except Exception:
        logging.exception("daily_runner failed")


if __name__ == "__main__":
    main()

import json
import logging
from datetime import datetime

from aviasales_fetcher import AviasalesFetcher
from deal_filter import filter_deals_by_score, travel_days
from steal_engine import is_steal
from notifier import send_telegram
from db import (
    insert_offer,
    mark_alert_sent,
    DB_FILE,
    get_last_30d_avg,
)

from config import Config

cfg = Config.from_json()


def format_msg(offer, diff_percent: int) -> str:
    """Return formatted Telegram alert message."""
    return (
        f"✈️ *STEAL!* {offer.origin}–{offer.destination} "
        f"{offer.price_pln} PLN  ({offer.depart_date}→{offer.return_date})\n"
        f"_{diff_percent}% poniżej średniej_  \n[Rezerwuj]({offer.deep_link})"
    )


def is_valid_round_trip(offer, cfg):
    if not offer.return_date:
        return False

    days = travel_days(offer.depart_date, offer.return_date)
    if not days:
        return False

    min_days = getattr(cfg, "min_trip_days", None)
    if min_days and days < min_days:
        return False
    max_days = getattr(cfg, "max_trip_days", None)
    if max_days and days > max_days:
        return False

    return True

def is_flight_duration_reasonable(offer, max_hours=15):
    # Placeholder logic: real implementation would require duration field from API
    # or separate lookup. For now we skip offers with certain airlines or links that contain layovers.
    too_long_indicators = ["layover", "overnight", "+1"]  # can be extended
    link = offer.deep_link.lower()
    return not any(token in link for token in too_long_indicators)

def run_once() -> None:
    """Fetch new offers once and process them."""
    fetcher = AviasalesFetcher(domain=getattr(cfg, "domain"))

    all_valid_offers = []
    for origin in getattr(cfg, "origins"):
        for dest in getattr(cfg, "destinations"):
            print(f"Fetching: {origin} ➔ {dest}")
            try:
                offers = fetcher.search_prices(
                    origin=origin,
                    destination=dest,
                    departure_at=None,
                    return_at=None,
                    one_way=getattr(cfg, "one_way", False),
                    currency=getattr(cfg, "currency", "PLN"),
                    limit=getattr(cfg, "limit", 100),
                    max_age_h=getattr(cfg, "hours", 12),
                )
            except Exception as ex:
                print(f"  Failed to fetch {origin}->{dest}: {ex}")
                continue

            for off in offers:
                if not getattr(cfg, "one_way", False):
                    if not is_valid_round_trip(off, cfg):
                        continue
                if not is_flight_duration_reasonable(off, getattr(cfg, "max_flight_duration_hours", 15)):
                    continue

                offer_id = insert_offer(off, db_path=DB_FILE)
                if is_steal(off, cfg) and not getattr(off, "alert_sent", False):
                    avg = get_last_30d_avg(off.origin, off.destination)
                    diff_percent = 0
                    if avg:
                        diff_percent = int(
                            100
                            * (
                                1
                                - float(off.price_pln)
                                / float(avg)
                            )
                        )
                    send_telegram(format_msg(off, diff_percent))
                    mark_alert_sent(offer_id, db_path=DB_FILE)
                    off.alert_sent = True

                all_valid_offers.append(off)

    if not all_valid_offers:
        print("No offers matched filters.")
        return

    filtered = filter_deals_by_score(all_valid_offers)
    fetcher.save_offers(filtered, backend=getattr(cfg, "save", "sqlite"))
    print(f"Saved {len(filtered)} filtered offers.")


def main() -> None:
    """Entry-point executed by ``tasks.py`` or cron."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        run_once()
    except Exception:
        logging.exception("daily_runner main failed")

if __name__ == "__main__":
    main()

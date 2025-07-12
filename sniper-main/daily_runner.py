import json
from datetime import datetime
import logging
from aviasales_fetcher import AviasalesFetcher
from deal_filter import filter_deals_by_score, travel_days
from steal_engine import is_steal
from notifier import send_telegram
from db import insert_offer, mark_alert_sent, DB_FILE, get_last_30d_avg
from config import Config


def format_msg(offer, diff_percent: int) -> str:
    """Return formatted Telegram alert message."""
    return (
        f"✈️ *STEAL!* {offer.origin}–{offer.destination} "
        f"{offer.price_pln} PLN  ({offer.depart_date}→{offer.return_date})\n"
        f"_{diff_percent}% poniżej średniej_  \n[Rezerwuj]({offer.deep_link})"
    )

def load_config(path="config.json"):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)

def is_valid_round_trip(offer, cfg):
    if not offer.return_date:
        return False

    days = travel_days(offer.depart_date, offer.return_date)
    if not days:
        return False

    if cfg.get("min_trip_days") and days < cfg["min_trip_days"]:
        return False
    if cfg.get("max_trip_days") and days > cfg["max_trip_days"]:
        return False

    return True

def is_flight_duration_reasonable(offer, max_hours=15):
    # Placeholder logic: real implementation would require duration field from API
    # or separate lookup. For now we skip offers with certain airlines or links that contain layovers.
    too_long_indicators = ["layover", "overnight", "+1"]  # can be extended
    link = offer.deep_link.lower()
    return not any(token in link for token in too_long_indicators)

def main():
    cfg = load_config()
    fetcher = AviasalesFetcher(domain=cfg.get("domain"))

    all_valid_offers = []
    for origin in cfg["origins"]:
        for dest in cfg["destinations"]:
            print(f"Fetching: {origin} ➔ {dest}")
            try:
                offers = fetcher.search_prices(
                    origin=origin,
                    destination=dest,
                    departure_at=None,
                    return_at=None,
                    one_way=cfg.get("one_way", False),
                    currency=cfg.get("currency", "PLN"),
                    limit=cfg.get("limit", 100),
                    max_age_h=cfg.get("hours", 12),
                )
            except Exception as ex:
                print(f"  Failed to fetch {origin}->{dest}: {ex}")
                continue

            for off in offers:
                if not cfg.get("one_way", False):
                    if not is_valid_round_trip(off, cfg):
                        continue
                if not is_flight_duration_reasonable(off, cfg.get("max_flight_duration_hours", 15)):
                    continue

                offer_id = insert_offer(off, db_path=DB_FILE)
                cfg_obj = Config.from_json()
                if is_steal(off, cfg_obj) and not getattr(off, "alert_sent", False):
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

                all_valid_offers.append(off)

    if not all_valid_offers:
        print("No offers matched filters.")
        return

    filtered = filter_deals_by_score(all_valid_offers)
    fetcher.save_offers(filtered, backend=cfg.get("save", "sqlite"))
    print(f"Saved {len(filtered)} filtered offers.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except Exception:
        logging.exception("daily_runner failed")

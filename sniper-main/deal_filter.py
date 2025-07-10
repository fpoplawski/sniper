from __future__ import annotations

from statistics import median
import sqlite3
from typing import Mapping, Any
from datetime import datetime, timezone

from geo import distance_km


def compute_baseline(db_path: str, origin: str, dest: str, days: int = 90) -> float:
    """Return median of daily minimal prices for given route in the last *days*.

    Prices are pulled from the ``flights`` table in the SQLite database located
    at ``db_path``.  The ``found_at`` timestamp is used to filter offers from
    the last ``days`` days.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MIN(price)
        FROM flights
        WHERE origin=? AND destination=?
          AND date(found_at) >= date('now', ?)
        GROUP BY date(found_at)
        """,
        (origin, dest, f'-{days} day'),
    )
    prices = [row[0] for row in cur.fetchall() if row[0] is not None]
    conn.close()
    if not prices:
        return 0.0
    return float(median(prices))


def compute_deal_score(row: Mapping[str, Any], baseline: float) -> float:
    """Return weighted score for offer in ``row`` compared to ``baseline``."""
    price = float(row.get("price", 0.0))
    if price <= 0 or baseline <= 0:
        return 0.0

    origin = row.get("origin", "")
    dest = row.get("destination", "")
    dist = distance_km(origin, dest)
    price_per_km = price / dist if dist else float("inf")

    score_price = (baseline - price) / baseline * 100.0
    score_ppk = 5.0 - price_per_km if dist > 0 else 0.0

    depart = row.get("depart_date") or row.get("depart")
    score_time = 0.0
    if depart:
        try:
            dt = datetime.fromisoformat(str(depart))
            now = datetime.now(dt.tzinfo or timezone.utc).date()
            days_until = (dt.date() - now).days
            score_time = 10.0 - abs(days_until - 30) / 3.0
        except Exception:
            score_time = 0.0

    score = 0.6 * score_price + 0.2 * score_ppk + 0.2 * score_time
    return round(score, 2)


def is_good(row: Mapping[str, Any], cfg: Mapping[str, Any], baseline: float) -> bool:
    """Return ``True`` if flight offer in ``row`` qualifies as a good deal."""
    price = float(row.get("price", 0.0))
    if price <= 0 or baseline <= 0:
        return False

    pax = int(row.get("passengers", 1))
    total = price * pax

    dist = distance_km(row.get("origin", ""), row.get("destination", ""))
    price_per_km = price / dist if dist else float("inf")

    if price > cfg.get("max_price", float("inf")):
        return False
    if cfg.get("max_price_total") and total > cfg.get("max_price_total"):
        return False
    if price_per_km > cfg.get("max_price_per_km", float("inf")):
        return False
    if cfg.get("excluded_airlines") and row.get("airline") in cfg.get("excluded_airlines"):
        return False
    score = compute_deal_score(row, baseline)
    if score < cfg.get("min_score", 0.0):
        return False
    return True


def is_good_composite(row: Mapping[str, Any], cfg: Mapping[str, Any], baseline: float) -> bool:
    """Return ``True`` if flight offer in ``row`` qualifies using a weighted composite score."""
    price = float(row.get("price", 0.0))
    if price <= 0 or baseline <= 0:
        return False

    pax = int(row.get("passengers", 1))
    total = price * pax

    origin = row.get("origin", "")
    dest = row.get("destination", "")
    dist = distance_km(origin, dest)
    price_per_km = price / dist if dist else float("inf")

    if price > cfg.get("max_price", float("inf")):
        return False
    if cfg.get("max_price_total") and total > cfg.get("max_price_total"):
        return False
    if cfg.get("excluded_airlines") and row.get("airline") in cfg.get("excluded_airlines"):
        return False

    def _clamp(val: float) -> float:
        return max(0.0, min(100.0, val))

    max_price = cfg.get("max_price", price)
    price_score = _clamp((max_price - price) / max_price * 100.0)

    max_ppk = cfg.get("max_price_per_km")
    if max_ppk and max_ppk > 0 and price_per_km != float("inf"):
        ppk_score = _clamp((max_ppk - price_per_km) / max_ppk * 100.0)
    else:
        ppk_score = 0.0 if price_per_km == float("inf") else 100.0

    baseline_score = _clamp((baseline - price) / baseline * 100.0)

    trip_days = row.get("trip_days")
    min_days = cfg.get("min_trip_days")
    max_days = cfg.get("max_trip_days")
    if trip_days is None:
        days_score = 100.0
    elif (min_days and trip_days < min_days) or (max_days and trip_days > max_days):
        days_score = 0.0
    else:
        days_score = 100.0

    w_price = cfg.get("weight_price", 0.4)
    w_ppk = cfg.get("weight_price_per_km", 0.3)
    w_baseline = cfg.get("weight_baseline_diff", 0.2)
    w_days = cfg.get("weight_trip_duration", 0.1)

    score = (
        w_price * price_score
        + w_ppk * ppk_score
        + w_baseline * baseline_score
        + w_days * days_score
    )

    if score < cfg.get("min_composite_score", 0.0):
        return False
    return True


__all__ = ["is_good", "is_good_composite", "compute_baseline", "compute_deal_score"]

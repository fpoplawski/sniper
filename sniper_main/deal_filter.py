from __future__ import annotations

from statistics import median
import sqlite3
from typing import Mapping, Any, List
from datetime import datetime, timezone, date

from .geo import distance_km


# ────────────────────────────────────────────────────────────────
# 1.  Funkcje analityczne używane w scoringu „legacy”
# ────────────────────────────────────────────────────────────────


def compute_baseline(
    db_path: str, origin: str, dest: str, days: int = 90
) -> float:
    """Zwróć medianę dziennych minimów cen dla trasy z ostatnich *days* dni."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='offers_raw'"
    )
    table = "offers_raw" if cur.fetchone() else "flights"
    price_col = "price_pln" if table == "offers_raw" else "price"
    query = f"""
        SELECT MIN({price_col})
          FROM {table}
         WHERE origin=? AND destination=?
           AND date(fetched_at) >= date('now', ?)
         GROUP BY date(fetched_at)
    """
    cur.execute(query, (origin, dest, f"-{days} day"))
    prices = [row[0] for row in cur.fetchall() if row[0] is not None]
    conn.close()
    if not prices:
        return 0.0
    return float(median(prices))


def compute_deal_score(row: Mapping[str, Any], baseline: float) -> float:
    """Zwróć ważony score oferty w porównaniu z baseline.

    (Im wyższy, tym lepiej.)
    """
    price = float(row.get("price_pln") or row.get("price", 0.0))
    if price <= 0 or baseline <= 0:
        return 0.0

    origin = row.get("origin", "")
    dest = row.get("destination", "")
    dist = distance_km(origin, dest)
    price_per_km = price / dist if dist else float("inf")

    score_price = (baseline - price) / baseline * 100.0
    score_ppk = 5.0 - price_per_km if dist > 0 else 0.0

    depart = row.get("depart_date")
    score_time = 0.0
    if depart:
        try:
            dt = datetime.fromisoformat(str(depart))
            now = datetime.now(dt.tzinfo or timezone.utc).date()
            days_until = (dt.date() - now).days
            score_time = 10.0 - abs(days_until - 30) / 3.0
        except Exception:
            pass

    score = 0.6 * score_price + 0.2 * score_ppk + 0.2 * score_time
    return round(score, 2)


def is_good(
    row: Mapping[str, Any], cfg: Mapping[str, Any], baseline: float
) -> bool:
    """Proste kryterium „dobra okazja".

    Ocenia maks. cenę, cenę/km i minimalny score.
    """
    price = float(row.get("price_pln") or row.get("price", 0.0))
    if price <= 0 or baseline <= 0:
        return False

    if cfg.get("max_price") and price > cfg["max_price"]:
        return False

    dist = distance_km(row.get("origin", ""), row.get("destination", ""))
    price_per_km = price / dist if dist else float("inf")
    if cfg.get("max_price_per_km") and price_per_km > cfg["max_price_per_km"]:
        return False

    score = compute_deal_score(row, baseline)
    if score < cfg.get("min_score", 0.0):
        return False
    return True


def is_good_composite(
    row: Mapping[str, Any], cfg: Mapping[str, Any], baseline: float
) -> bool:
    """Composite criterion used in tests combining several checks."""
    price = float(row.get("price", row.get("price_pln", 0.0)))
    if price <= 0 or baseline <= 0:
        return False

    if cfg.get("max_price") and price > cfg["max_price"]:
        return False

    dist = distance_km(row.get("origin", ""), row.get("destination", ""))
    price_per_km = price / dist if dist else float("inf")
    if cfg.get("max_price_per_km") and price_per_km > cfg["max_price_per_km"]:
        return False

    trip_days = row.get("trip_days")
    if trip_days is not None:
        if cfg.get("min_trip_days") and trip_days < cfg["min_trip_days"]:
            return False
        if cfg.get("max_trip_days") and trip_days > cfg["max_trip_days"]:
            return False

    if (
        cfg.get("excluded_airlines")
        and row.get("airline") in cfg["excluded_airlines"]
    ):
        return False

    diff_pct = (baseline - price) / baseline * 100.0
    score = 1.5 * diff_pct + (5.0 - price_per_km)
    if score < cfg.get("min_composite_score", 0.0):
        return False

    return True


# ────────────────────────────────────────────────────────────────
# 2.  „Zaślepki” wymagane przez daily_runner.py
# ────────────────────────────────────────────────────────────────


def filter_deals_by_score(
    offers: List[Mapping[str, Any]], cfg: Mapping[str, Any]
) -> List[Mapping[str, Any]]:
    """
    Tymczasowy filtr używany przez daily_runner.py.

    Obecnie po prostu zwraca niezmodyfikowaną listę ofert.
    Jeżeli zechcesz ponownie wprowadzić szczegółowy scoring,
    zaimplementuj tu dowolną logikę i zwróć przefiltrowaną listę.
    """
    # Przykład użycia compute_deal_score():
    # baseline = compute_baseline(
    #     DB_FILE, offer["origin"], offer["destination"]
    # )
    # if compute_deal_score(offer, baseline) >= cfg["min_score"]:
    #     keep…
    return offers


def travel_days(depart_date: date, return_date: date | None) -> int:
    """
    Oblicz liczbę dni pobytu od daty wylotu do daty powrotu.

    Jeśli ``return_date`` jest ``None`` (lot w jedną stronę),
    funkcja zwraca 0, aby daily_runner mógł łatwo odfiltrować
    oferty one-way, gdy oczekiwany jest trip RT.
    """
    if not depart_date or not return_date:
        return 0
    return (return_date - depart_date).days


# ────────────────────────────────────────────────────────────────
# 3.  Eksport symboli
# ────────────────────────────────────────────────────────────────

__all__ = [
    "compute_baseline",
    "compute_deal_score",
    "is_good_composite",
    "is_good",
    "filter_deals_by_score",
    "travel_days",
]

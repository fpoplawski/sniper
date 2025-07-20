from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
from typing import Any

import pandas as pd

from .db import DB_FILE
from .models import FlightOffer

logger = logging.getLogger(__name__)


def is_weekday_steal(offer: FlightOffer, cfg: Any) -> bool:
    """Return ``True`` if *offer* price is far below the seasonal average."""

    weekday = offer.depart_date.weekday()
    weekday_sql = str((weekday + 1) % 7)

    # Seasonal average for this route/weekday
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.execute(
            """
            SELECT avg_price FROM weekday_avg
             WHERE origin=? AND destination=? AND weekday=?
            """,
            (offer.origin, offer.destination, weekday),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        return False

    avg_price = Decimal(str(row[0]))

    # Standard deviation of historical prices for this route/weekday
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(
            """
            SELECT price_pln FROM offers_raw
             WHERE origin=? AND destination=?
               AND strftime('%w', depart_date)=?
               AND fetched_at >= DATE('now', '-90 days')
            """,
            conn,
            params=(offer.origin, offer.destination, weekday_sql),
        )

    std_val = df["price_pln"].std()
    if pd.isna(std_val):
        std = Decimal("0")
    else:
        std = Decimal(str(std_val))

    threshold = avg_price - Decimal(str(cfg.steal_threshold)) * std
    return offer.price_pln < threshold

__all__ = ["is_weekday_steal"]

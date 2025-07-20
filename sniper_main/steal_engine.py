from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
import sqlite3
import pandas as pd

from .db import DB_FILE
from .models import FlightOffer

logger = logging.getLogger(__name__)


def is_weekday_steal(
    offer: FlightOffer, cfg: Any, *, db_path: str = DB_FILE
) -> bool:
    """Return ``True`` if price is a steal vs weekday average."""

    weekday = offer.depart_date.weekday()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "SELECT avg_price FROM weekday_avg WHERE origin=? AND destination=? AND weekday=?",
            (offer.origin, offer.destination, weekday),
        )
        row = cur.fetchone()
        if not row:
            return False
        avg_price = Decimal(str(row[0]))

        df = pd.read_sql_query(
            """
            SELECT price_pln FROM offers_raw
             WHERE origin=? AND destination=?
               AND strftime('%w', depart_date)=?
               AND fetched_at >= DATE('now', '-90 day')
            """,
            conn,
            params=(offer.origin, offer.destination, str(weekday)),
        )

    if df.empty:
        return False

    std = Decimal(str(df["price_pln"].astype(float).std(ddof=0) or 0))
    k = Decimal(str(getattr(cfg, "steal_threshold", 1)))
    threshold = avg_price - k * std
    return offer.price_pln < threshold

__all__ = ["is_weekday_steal"]

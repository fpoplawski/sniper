from __future__ import annotations

import os
import sqlite3
import pathlib
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from models import FlightOffer


# Default paths – relative to the repository root
REPO_DIR = pathlib.Path(__file__).resolve().parent.parent
DB_FILE = os.getenv("SNIPER_DB", str(REPO_DIR / "aviasales_offers.db"))
SCHEMA_FILE = str(REPO_DIR / "schema.sql")


def init_db(db_path: str = DB_FILE, schema_path: str = SCHEMA_FILE) -> None:
    """Initialize SQLite database using *schema_path*."""
    conn = sqlite3.connect(db_path)
    with open(schema_path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())
    conn.commit()
    conn.close()


def insert_offer(offer: FlightOffer, db_path: str = DB_FILE) -> int:
    """Insert *offer* into ``offers_raw`` and return its row id."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id FROM offers_raw
         WHERE origin=? AND destination=? AND depart_date=? AND return_date IS ?
           AND price_pln=? AND airline=? AND stops=? AND deep_link=?
        """,
        (
            offer.origin,
            offer.destination,
            offer.depart_date.isoformat(),
            offer.return_date.isoformat() if offer.return_date else None,
            float(offer.price_pln),
            offer.airline,
            offer.stops,
            offer.deep_link,
        ),
    )
    existing = cur.fetchone()
    if existing:
        conn.close()
        return existing[0]

    cur.execute(
        """
        INSERT INTO offers_raw(
            origin, destination, depart_date, return_date, price_pln, airline,
            stops, total_time_h, layover_h, deep_link, fetched_at, alert_sent
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            offer.origin,
            offer.destination,
            offer.depart_date.isoformat(),
            offer.return_date.isoformat() if offer.return_date else None,
            float(offer.price_pln),
            offer.airline,
            offer.stops,
            offer.total_flight_time_h,
            offer.max_layover_h,
            offer.deep_link,
            offer.fetched_at.isoformat(),
            int(offer.alert_sent),
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def mark_alert_sent(offer_id: int, db_path: str = DB_FILE) -> None:
    """Mark offer with ``offer_id`` as having an alert sent."""
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE offers_raw SET alert_sent=1 WHERE id=?", (offer_id,))
    conn.commit()
    conn.close()


def get_last_30d_avg(origin: str, dest: str, db_path: str = DB_FILE) -> Optional[Decimal]:
    """Return average price for ``origin``-``dest`` from the last 30 days."""
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        """
        SELECT AVG(mean_price)
        FROM offers_agg
        WHERE origin=? AND destination=?
          AND date(day) >= date('now', '-30 day')
        """,
        (origin, dest),
    )
    val = cur.fetchone()[0]
    conn.close()
    return Decimal(str(val)) if val is not None else None


def upsert_daily_avg(origin: str, dest: str, mean_price: Decimal | float, db_path: str = DB_FILE) -> None:
    """Insert or update today's average price for ``origin``-``dest``."""
    day_str = datetime.utcnow().replace(tzinfo=timezone.utc).date().isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO offers_agg (origin, destination, day, mean_price)
        VALUES (?,?,?,?)
        ON CONFLICT(origin, destination, day)
        DO UPDATE SET mean_price=excluded.mean_price
        """,
        (origin, dest, day_str, str(mean_price)),
    )
    conn.commit()
    conn.close()


__all__ = [
    "init_db",
    "insert_offer",
    "mark_alert_sent",
    "get_last_30d_avg",
    "upsert_daily_avg",
]

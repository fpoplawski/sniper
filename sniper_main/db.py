from __future__ import annotations

import logging
import os
import sqlite3
import pathlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List, Tuple

from .models import FlightOffer


# Default paths – relative to the repository root
REPO_DIR = pathlib.Path(__file__).resolve().parent.parent
DB_FILE = os.getenv("SNIPER_DB", str(REPO_DIR / "aviasales_offers.db"))
SCHEMA_FILE = str(REPO_DIR / "schema.sql")
SCHEMA_VERSION = 1

logger = logging.getLogger(__name__)


def migrate(db_path: str = DB_FILE, schema_path: str = SCHEMA_FILE) -> None:
    """Run pending migrations on the database."""
    logger.info("Running migrations for %s", db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version "
            "(version INTEGER NOT NULL)"
        )
        cur = conn.execute("SELECT version FROM schema_version")
        row = cur.fetchone()
        current = row[0] if row else 0
        if current < SCHEMA_VERSION:
            logger.info("Applying schema version %s", SCHEMA_VERSION)
            with open(schema_path, "r", encoding="utf-8") as fh:
                conn.executescript(fh.read())
            if row:
                conn.execute(
                    "UPDATE schema_version SET version=?", (SCHEMA_VERSION,)
                )
            else:
                conn.execute(
                    "INSERT INTO schema_version(version) VALUES (?)",
                    (SCHEMA_VERSION,),
                )
            conn.commit()


def init_db(db_path: str = DB_FILE, schema_path: str = SCHEMA_FILE) -> None:
    """Initialize SQLite database using *schema_path*."""
    logger.info("Initializing database at %s", db_path)
    with sqlite3.connect(db_path) as conn:
        with open(schema_path, "r", encoding="utf-8") as fh:
            conn.executescript(fh.read())
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version "
            "(version INTEGER NOT NULL)"
        )
        conn.execute("DELETE FROM schema_version")
        conn.execute(
            "INSERT INTO schema_version(version) VALUES (?)",
            (SCHEMA_VERSION,),
        )


def insert_offer(offer: FlightOffer, db_path: str = DB_FILE) -> int:
    """Insert *offer* into ``offers_raw`` and return its row id."""
    logger.info(
        "Inserting offer %s ➔ %s on %s",
        offer.origin,
        offer.destination,
        offer.depart_date,
    )
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id FROM offers_raw
             WHERE origin=? AND destination=? AND depart_date=?
               AND return_date IS ?
               AND price_pln=? AND airline=? AND stops=?
               AND deep_link=?
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
            return existing[0]

        cur.execute(
            """
            INSERT INTO offers_raw(
                origin,
                destination,
                depart_date,
                return_date,
                price_pln,
                airline,
                stops,
                total_time_h,
                layover_h,
                deep_link,
                fetched_at,
                alert_sent
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
        return row_id


def mark_alert_sent(offer_id: int, db_path: str = DB_FILE) -> None:
    """Mark offer with ``offer_id`` as having an alert sent."""
    logger.info("Marking alert sent for offer %s", offer_id)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE offers_raw SET alert_sent=1 WHERE id=?", (offer_id,)
        )
        conn.commit()


def get_last_30d_avg(
    origin: str, dest: str, db_path: str = DB_FILE
) -> Optional[Decimal]:
    """Return average price for ``origin``-``dest`` from the last 30 days."""
    logger.info("Querying 30-day average for %s-%s", origin, dest)
    with sqlite3.connect(db_path) as conn:
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
    return Decimal(str(val)) if val is not None else None


def upsert_daily_avg(
    origin: str, dest: str, mean_price: Decimal | float, db_path: str = DB_FILE
) -> None:
    """Insert or update today's average price for ``origin``-``dest``."""
    day_str = datetime.utcnow().replace(tzinfo=timezone.utc).date().isoformat()
    logger.info("Upserting daily avg for %s-%s", origin, dest)
    with sqlite3.connect(db_path) as conn:
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


def insert_pair(
    out_id: int,
    in_id: int,
    price_total: float,
    origin: str,
    dest: str,
    depart: str,
    ret: str,
    steal: bool,
    db_path: str = DB_FILE,
) -> int:
    """Insert a paired one-way offer.

    Return its row id or ``-1`` if duplicate.
    """

    sql = """
    INSERT INTO offers_pair (
        out_id,in_id,origin,destination,
        depart_date,return_date,
        price_total_pln,steal_pair,fetched_at
    ) VALUES (?,?,?,?,?,?,?,?,
              CURRENT_TIMESTAMP)
    ON CONFLICT(out_id,in_id) DO NOTHING
    RETURNING id;
    """
    logger.info("Inserting pair %s/%s", out_id, in_id)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            sql,
            (
                out_id,
                in_id,
                origin,
                dest,
                depart,
                ret,
                price_total,
                int(steal),
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row[0]) if row else -1


def find_returns(
    out_offer_id: int,
    dest: str,
    orig: str,
    window_start: str,
    window_end: str,
    max_stops: int,
    db_path: str = DB_FILE,
) -> List[Tuple]:
    """Find return legs for a given offer within a time window."""

    q = """
    SELECT id, price_pln, departure_at
      FROM offers_raw
     WHERE origin      = ?
       AND destination = ?
       AND departure_at BETWEEN ? AND ?
       AND stops <= ?
    """
    logger.info("Searching return legs for offer %s", out_offer_id)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            q,
            (dest, orig, window_start, window_end, max_stops),
        ).fetchall()
        return rows


__all__ = [
    "init_db",
    "insert_offer",
    "mark_alert_sent",
    "get_last_30d_avg",
    "upsert_daily_avg",
    "insert_pair",
    "migrate",
    "find_returns",
]

import os
import sqlite3
from datetime import datetime, timezone, date
from decimal import Decimal

from sniper_main.db import init_db, insert_offer
from sniper_main.models import FlightOffer


def make_offer() -> FlightOffer:
    return FlightOffer(
        origin="WAW",
        destination="JFK",
        depart_date=date(2024, 9, 10),
        return_date=date(2024, 9, 20),
        price_pln=Decimal("500"),
        airline="AA",
        stops=0,
        total_flight_time_h=None,
        max_layover_h=None,
        deep_link="https://example.com/offer",
        fetched_at=datetime.now(timezone.utc),
    )


def test_insert_offer_deduplicates(tmp_path):
    db_file = tmp_path / "test.db"
    migrations_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "sniper_main",
        "migrations",
    )
    init_db(str(db_file), migrations_dir=migrations_dir)

    offer = make_offer()
    first_id = insert_offer(offer, db_path=str(db_file))
    second_id = insert_offer(offer, db_path=str(db_file))

    assert first_id == second_id

    conn = sqlite3.connect(db_file)
    cur = conn.execute("SELECT COUNT(*) FROM offers_raw")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 1

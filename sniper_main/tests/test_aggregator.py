import os
from datetime import datetime, timedelta
import sqlite3

import pytest

from sniper_main.db import init_db
from sniper_main import aggregator


def test_aggregate_30_days_no_gaps(tmp_path):
    db_file = tmp_path / "test.db"
    migrations_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "sniper_main",
        "migrations",
    )
    init_db(str(db_file), migrations_dir=migrations_dir)

    conn = sqlite3.connect(db_file)
    now = datetime.utcnow()
    prices = []
    for i in range(30):
        dt = now - timedelta(days=i)
        price = 100 + i
        prices.append(price)
        conn.execute(
            "INSERT INTO offers_raw (origin, destination, price_pln, "
            "fetched_at) VALUES (?,?,?,?)",
            ("WAW", "JFK", price, dt.isoformat()),
        )
    conn.commit()
    conn.close()

    aggregator.aggregate(str(db_file))

    conn = sqlite3.connect(db_file)
    cur = conn.execute(
        "SELECT mean_price FROM offers_agg WHERE origin='WAW' "
        "AND destination='JFK'"
    )
    row = cur.fetchone()
    conn.close()

    assert row is not None
    expected_avg = sum(prices) / len(prices)
    assert float(row[0]) == pytest.approx(expected_avg)

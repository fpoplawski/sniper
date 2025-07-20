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


def test_compute_weekday_averages(tmp_path):
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
    monday = now - timedelta(days=now.weekday())
    tuesday = monday + timedelta(days=1)

    conn.execute(
        "INSERT INTO offers_raw (origin, destination, depart_date, price_pln, fetched_at)"
        " VALUES (?,?,?,?,?)",
        ("WAW", "JFK", monday.date().isoformat(), 100, now.isoformat()),
    )
    conn.execute(
        "INSERT INTO offers_raw (origin, destination, depart_date, price_pln, fetched_at)"
        " VALUES (?,?,?,?,?)",
        ("WAW", "JFK", monday.date().isoformat(), 200, now.isoformat()),
    )
    conn.execute(
        "INSERT INTO offers_raw (origin, destination, depart_date, price_pln, fetched_at)"
        " VALUES (?,?,?,?,?)",
        ("WAW", "JFK", tuesday.date().isoformat(), 300, now.isoformat()),
    )
    conn.commit()
    conn.close()

    df = aggregator.compute_weekday_averages(str(db_file))
    mon = monday.weekday()
    row = df[(df["origin"] == "WAW") & (df["destination"] == "JFK") & (df["weekday"] == mon)]
    assert not row.empty
    assert float(row.iloc[0]["avg_price"]) == pytest.approx(150.0)

    aggregator.store_weekday_averages(str(db_file))
    conn = sqlite3.connect(db_file)
    cur = conn.execute(
        "SELECT avg_price FROM weekday_avg WHERE origin='WAW' AND destination='JFK' AND weekday=?",
        (mon,),
    )
    db_row = cur.fetchone()
    conn.close()
    assert db_row is not None
    assert float(db_row[0]) == pytest.approx(150.0)

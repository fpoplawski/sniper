import os
import sqlite3
from datetime import datetime, timezone, timedelta
from tempfile import NamedTemporaryFile

import pytest

from sniper_main.deal_filter import (
    compute_baseline,
    compute_deal_score,
    is_good_composite,
)
from sniper_main.geo import distance_km


def setup_db(records):
    temp = NamedTemporaryFile(suffix=".db", delete=False)
    path = temp.name
    temp.close()
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE flights (origin TEXT, destination TEXT, "
        "price REAL, fetched_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO flights VALUES (?, ?, ?, ?)",
        records,
    )
    conn.commit()
    conn.close()
    return path


def test_compute_baseline_and_score(tmp_path):
    now = datetime.now(timezone.utc)
    records = []
    for i, price in enumerate([100, 150]):
        records.append(
            ("FRA", "HAM", price, (now - timedelta(days=2)).isoformat())
        )
    for i, price in enumerate([200, 250]):
        records.append(
            ("FRA", "HAM", price, (now - timedelta(days=1)).isoformat())
        )
    for i, price in enumerate([300, 400]):
        records.append(("FRA", "HAM", price, now.isoformat()))

    db_path = setup_db(records)
    baseline = compute_baseline(db_path, "FRA", "HAM", days=10)
    os.unlink(db_path)
    assert baseline == pytest.approx(200.0)

    depart = (now + timedelta(days=30)).isoformat()
    row = {
        "origin": "FRA",
        "destination": "HAM",
        "price": 150.0,
        "depart_date": depart,
    }
    score = compute_deal_score(row, baseline)
    dist = distance_km("FRA", "HAM")
    expected = 0.6 * ((baseline - 150.0) / baseline * 100.0)
    expected += 0.2 * (5.0 - 150.0 / dist)
    expected += 0.2 * (10.0 - abs(30 - 30) / 3.0)
    expected = round(expected, 2)
    assert score == pytest.approx(expected)


def test_is_good_composite(tmp_path):
    baseline = 200.0
    depart = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    row = {
        "origin": "FRA",
        "destination": "HAM",
        "price": 150.0,
        "depart_date": depart,
    }
    row["trip_days"] = 7
    cfg = {
        "max_price": 180.0,
        "max_price_per_km": 2.0,
        "min_composite_score": 40.0,
        "min_trip_days": 5,
        "max_trip_days": 10,
    }
    assert is_good_composite(row, cfg, baseline)

    row_bad = {
        "origin": "FRA",
        "destination": "HAM",
        "price": 250.0,
        "depart_date": depart,
    }
    row_bad["trip_days"] = 7
    assert not is_good_composite(row_bad, cfg, baseline)

    row_excluded = {
        "origin": "FRA",
        "destination": "HAM",
        "price": 150.0,
        "depart_date": depart,
        "airline": "FR",
    }
    row_excluded["trip_days"] = 7
    cfg_ex = {
        "max_price": 180.0,
        "max_price_per_km": 2.0,
        "min_composite_score": 40.0,
        "min_trip_days": 5,
        "max_trip_days": 10,
        "excluded_airlines": ["FR", "W6"],
    }
    assert not is_good_composite(row_excluded, cfg_ex, baseline)

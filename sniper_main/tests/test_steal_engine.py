from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import sqlite3
from datetime import datetime

from sniper_main import steal_engine


@dataclass(slots=True)
class Offer:
    origin: str
    destination: str
    depart_date: date
    price_pln: Decimal


@dataclass(slots=True)
class Config:
    steal_threshold: float = 0.2


def setup_db(tmp_path, prices):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE weekday_avg (origin TEXT, destination TEXT, weekday INTEGER, avg_price NUMERIC, PRIMARY KEY(origin,destination,weekday))"
    )
    conn.execute(
        "CREATE TABLE offers_raw (origin TEXT, destination TEXT, depart_date DATE, price_pln NUMERIC, fetched_at DATETIME)"
    )
    for price in prices:
        conn.execute(
            "INSERT INTO offers_raw VALUES (?,?,?,?,?)",
            ("WAW", "JFK", "2024-01-03", price, datetime.utcnow().isoformat()),
        )
    conn.execute(
        "INSERT INTO weekday_avg VALUES (?,?,?,?)",
        ("WAW", "JFK", 2, 1000.0),
    )
    conn.commit()
    conn.close()
    return db_file


def test_is_weekday_steal_true(tmp_path, monkeypatch):
    db_path = setup_db(tmp_path, [800, 1000, 1200])
    monkeypatch.setattr(steal_engine, "DB_FILE", str(db_path))

    offer = Offer("WAW", "JFK", date(2024, 1, 3), Decimal("900"))
    cfg = Config()
    assert steal_engine.is_weekday_steal(offer, cfg)


def test_is_weekday_steal_false_price(tmp_path, monkeypatch):
    db_path = setup_db(tmp_path, [800, 1000, 1200])
    monkeypatch.setattr(steal_engine, "DB_FILE", str(db_path))

    offer = Offer("WAW", "JFK", date(2024, 1, 3), Decimal("970"))
    cfg = Config()
    assert not steal_engine.is_weekday_steal(offer, cfg)


def test_is_weekday_steal_no_data(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE weekday_avg (origin TEXT, destination TEXT, weekday INTEGER, avg_price NUMERIC, PRIMARY KEY(origin,destination,weekday))"
    )
    conn.execute(
        "CREATE TABLE offers_raw (origin TEXT, destination TEXT, depart_date DATE, price_pln NUMERIC, fetched_at DATETIME)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(steal_engine, "DB_FILE", str(db_path))

    offer = Offer("WAW", "JFK", date(2024, 1, 3), Decimal("50"))
    cfg = Config()
    assert not steal_engine.is_weekday_steal(offer, cfg)


def test_is_weekday_steal_non_positive_avg(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE weekday_avg (origin TEXT, destination TEXT, weekday INTEGER, avg_price NUMERIC, PRIMARY KEY(origin,destination,weekday))"
    )
    conn.execute(
        "INSERT INTO weekday_avg VALUES (?,?,?,?)",
        ("WAW", "JFK", 2, 0.0),
    )
    conn.execute(
        "CREATE TABLE offers_raw (origin TEXT, destination TEXT, depart_date DATE, price_pln NUMERIC, fetched_at DATETIME)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(steal_engine, "DB_FILE", str(db_path))

    offer = Offer("WAW", "JFK", date(2024, 1, 3), Decimal("10"))
    cfg = Config()
    assert not steal_engine.is_weekday_steal(offer, cfg)

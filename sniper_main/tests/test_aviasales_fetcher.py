import os
import sys
import sqlite3
from datetime import datetime, timezone, timedelta
from tempfile import NamedTemporaryFile
from unittest.mock import patch, Mock

import logging


from sniper_main.aviasales_fetcher import AviasalesFetcher, main
from sniper_main.deal_filter import compute_deal_score


def make_payload():
    now = datetime.now(timezone.utc)
    return {
        "success": True,
        "data": [
            {
                "origin": "WAW",
                "destination": "JFK",
                "depart_date": "2024-09-10",
                "return_date": "2024-09-20",
                "price": 500,
                "airline": "AA",
                "found_at": now.isoformat(),
                "link": "/flight1",
            },
            {
                "origin": "WAW",
                "destination": "LAX",
                "depart_date": "2024-09-15",
                "return_date": "2024-09-25",
                "price": 700,
                "airline": "DL",
                "found_at": (now + timedelta(minutes=1)).isoformat(),
                "link": "/flight2",
            },
        ],
    }


def make_incomplete_payload():
    now = datetime.now(timezone.utc)
    return {
        "success": True,
        "data": [
            {
                "origin": "WAW",
                "destination": "JFK",
                "depart_date": "2024-09-10",
                "return_date": "2024-09-20",
                "price": 500,
                "airline": "AA",
                "link": "/f1",  # missing found_at
            },
            {
                "origin": "WAW",
                "destination": "LAX",
                "depart_date": "2024-09-15",
                "return_date": "2024-09-25",
                "price": 700,
                "airline": "DL",
                "found_at": "bad-date",
                "link": "/f2",
            },
            {
                "origin": "WAW",
                "destination": "BKK",
                "depart_date": "2024-10-01",
                "return_date": "2024-10-10",
                "price": 800,
                "airline": "BA",
                "found_at": now.isoformat(),
                # missing link
            },
        ],
    }


@patch("requests.get")
def test_fetch_and_save(mock_get):
    os.environ["TP_TOKEN"] = "x"
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = make_payload()
    mock_get.return_value = mock_resp

    fetcher = AviasalesFetcher()
    offers = fetcher.search_prices("WAW", max_age_h=24)
    assert len(offers) == 2
    for off in offers:
        assert off.deep_link.startswith("https://www.aviasales.com/flight")

    with NamedTemporaryFile(suffix=".db") as tmp:
        base = tmp.name[:-3]
        fetcher.save_offers(offers, "sqlite", path=base)
        conn = sqlite3.connect(base + ".db")
        cur = conn.execute("SELECT COUNT(*) FROM flights")
        count = cur.fetchone()[0]
        conn.close()
    assert count == 2


@patch("requests.get")
def test_skip_incomplete_rows(mock_get):
    os.environ["TP_TOKEN"] = "x"
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = make_incomplete_payload()
    mock_get.return_value = mock_resp

    fetcher = AviasalesFetcher()
    offers = fetcher.search_prices("WAW", max_age_h=24)
    assert len(offers) == 1
    assert offers[0].deep_link.startswith("https://www.aviasales.com/f1")
    assert isinstance(offers[0].fetched_at, datetime)


@patch("sniper_main.aviasales_fetcher.AviasalesFetcher.search_prices")
def test_cli_return_date_argument(mock_search, monkeypatch, caplog):
    os.environ["TP_TOKEN"] = "x"
    mock_search.return_value = []
    argv = ["aviasales_fetcher.py", "WAW", "--return-date", "2024-09-20"]
    caplog.set_level(logging.INFO)
    with patch.object(sys, "argv", argv):
        main()
    mock_search.assert_called_once_with(
        origin="WAW",
        destination=None,
        departure_at=None,
        return_at="2024-09-20",
        one_way=False,
        currency="PLN",
        limit=100,
        max_age_h=12,
    )
    assert any(
        "No offers found" in record.getMessage() for record in caplog.records
    )


def test_compute_deal_score_far_departure():
    """Ensure compute_deal_score handles distant departure correctly."""
    depart = (datetime.now(timezone.utc) + timedelta(days=120)).isoformat()
    row = {
        "origin": "FRA",
        "destination": "HAM",
        "price": 100.0,
        "depart_date": depart,
    }
    score = compute_deal_score(row, baseline=1000.0)
    assert score > 50

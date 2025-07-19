from __future__ import annotations

import datetime as dt
from decimal import Decimal
import sqlite3
import os

import requests
from dotenv import load_dotenv

from .models import FlightOffer

load_dotenv()


class AviasalesFetcherError(RuntimeError):
    """Błąd w komunikacji z Travelpayouts API."""


class AviasalesFetcher:
    """
    Klient Flight Data API v3 (ścieżka */aviasales/v3*).
    """

    def __init__(
        self,
        token: str | None = None,
        marker: str | int | None = None,
        base_url: str = "https://api.travelpayouts.com/aviasales/v3",
        domain: str = "https://www.aviasales.com",
    ) -> None:
        self.token = token or os.getenv("TP_TOKEN", "")
        self.marker = marker or os.getenv("TP_MARKER", "")
        self.base_url = base_url.rstrip("/")
        self.domain = domain.rstrip("/")

    # ──────────────────────────────────────────────────────────

    def search_prices(
        self,
        origin: str,
        destination: str | None = None,
        departure_at: str | None = None,
        return_at: str | None = None,
        *,
        one_way: bool = False,
        currency: str = "pln",
        limit: int = 100,
        max_age_h: int = 12,
    ) -> list[FlightOffer]:
        """Return a list of offers for a given route."""

        dest_param = destination or ""
        url = (
            f"{self.base_url}/prices_for_dates?"
            f"origin={origin}&destination={dest_param}&currency={currency}"
            f"&token={self.token}"
        )
        if departure_at:
            url += f"&departure_at={departure_at}"
        if return_at:
            url += f"&return_at={return_at}"
        url += f"&limit={limit}&one_way={'true' if one_way else 'false'}"
        url += f"&max_age={max_age_h}"
        if self.marker:
            url += f"&marker={self.marker}"

        resp = requests.get(url, timeout=15, headers={"Accept-Encoding": "gzip"})
        if resp.status_code != 200:
            raise AviasalesFetcherError(
                f"HTTP {resp.status_code} – {resp.text[:120]}"
            )

        data = resp.json()
        if not data.get("success"):
            raise AviasalesFetcherError(f"API error: {data.get('error')}")

        offers = [self._to_offer(item) for item in data.get("data", [])]
        return [off for off in offers if off]

    def save_offers(self, offers: list[FlightOffer], backend: str, *, path: str) -> None:
        """Persist offers to a SQLite DB (used in tests)."""
        if backend != "sqlite":
            raise ValueError("Unsupported backend")

        conn = sqlite3.connect(path + ".db")
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS flights (origin TEXT, destination TEXT, price REAL, fetched_at TEXT)"
        )
        rows = [
            (
                off.origin,
                off.destination,
                float(off.price_pln),
                off.fetched_at.isoformat(),
            )
            for off in offers
        ]
        cur.executemany("INSERT INTO flights VALUES (?,?,?,?)", rows)
        conn.commit()
        conn.close()

    def _to_offer(self, item: dict) -> FlightOffer | None:
        """Mapuje rekord JSON na obiekt FlightOffer."""
        if not item.get("link"):
            return None

        found_raw = item.get("found_at")
        if found_raw:
            try:
                fetched_at = dt.datetime.fromisoformat(found_raw)
            except Exception:
                return None
        else:
            fetched_at = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

        price_pln = Decimal(str(item["price"]))

        dep_raw = item.get("departure_at") or item.get("depart_date")
        ret_raw = item.get("return_at") or item.get("return_date")

        depart = dt.date.fromisoformat(dep_raw[:10])
        return_dt = dt.date.fromisoformat(ret_raw[:10]) if ret_raw else None

        deep_link = (
            f"{self.domain}{item['link']}" if item.get("link") else
            f"{self.domain}/search/"
            f"{item['origin']}{depart.strftime('%d%m')}"
            f"{item['destination']}"
            f"{return_dt.strftime('%d%m') if return_dt else ''}"
            f"1?marker={self.marker}"
        )

        return FlightOffer(
            origin=item["origin"],
            destination=item["destination"],
            depart_date=depart,
            return_date=return_dt,
            price_pln=price_pln,
            airline=item.get("airline", ""),
            stops=int(item.get("number_of_changes", 0)),
            total_flight_time_h=None,
            max_layover_h=None,
            deep_link=deep_link,
            fetched_at=fetched_at,
        )


def main(argv: list[str] | None = None) -> None:
    """Simple CLI used in tests."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("origin")
    parser.add_argument("destination", nargs="?")
    parser.add_argument("--departure-date", dest="departure", default=None)
    parser.add_argument("--return-date", dest="return_date", default=None)
    parser.add_argument("--one-way", action="store_true")
    parser.add_argument("--currency", default="PLN")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-age-h", type=int, default=12)
    args = parser.parse_args(argv)

    fetcher = AviasalesFetcher()
    offers = fetcher.search_prices(
        origin=args.origin,
        destination=args.destination,
        departure_at=args.departure,
        return_at=args.return_date,
        one_way=args.one_way,
        currency=args.currency,
        limit=args.limit,
        max_age_h=args.max_age_h,
    )

    if not offers:
        print("No offers found")
    else:
        for off in offers:
            print(off)


__all__ = ["AviasalesFetcher", "AviasalesFetcherError", "main"]

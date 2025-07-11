from __future__ import annotations

import argparse
import csv
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from typing import Iterable, List, Literal, Optional, TYPE_CHECKING

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ModuleNotFoundError:
    pass

if TYPE_CHECKING:
    from tabulate import tabulate  # noqa: F401

import requests
from geo import distance_km
from config import Config

CFG = Config()

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0 Safari/537.36"
)
BASE_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
    "User-Agent": UA,
}


class AviasalesFetcherError(Exception):
    ...


@dataclass(slots=True)
class FlightOffer:
    origin: str
    destination: str
    depart_date: date
    return_date: Optional[date]
    price_pln: Decimal
    airline: str
    stops: int
    total_flight_time_h: float
    max_layover_h: float
    deep_link: str
    fetched_at: datetime


class AviasalesFetcher:
    def __init__(
        self,
        token: Optional[str] = None,
        marker: Optional[str | int] = None,
        base_url: str = "https://api.travelpayouts.com/aviasales/v3",
        domain: str = "https://www.aviasales.com",
    ) -> None:
        self.token = token or os.getenv("TP_TOKEN")
        if not self.token:
            raise AviasalesFetcherError("TP_TOKEN env-var missing")
        self.marker = str(marker or os.getenv("TP_MARKER", "0"))
        self.base_url = base_url.rstrip("/")
        self.domain = domain.rstrip("/")

    def _build_url(self, rel: str) -> str:
        sep = "&" if "?" in rel else "?"
        return f"{self.domain}{rel}{sep}marker={self.marker}"

    @staticmethod
    def _within_age(fetched_at: str, max_h: int) -> bool:
        try:
            dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        except Exception:
            return False
        return datetime.now(timezone.utc) - dt <= timedelta(hours=max_h)

    def _build_params(
        self,
        origin: str,
        destination: Optional[str] = None,
        departure_at: Optional[str] = None,
        return_at: Optional[str] = None,
        one_way: bool = True,
        currency: str = "PLN",
        limit: int = 100,
        max_stops: Optional[int] = None,
    ) -> dict[str, str]:
        """Return request parameters for Aviasales API."""
        params: dict[str, str] = {
            "origin": origin,
            "currency": currency,
            "limit": str(limit),
            "token": self.token,
        }

        if destination:
            params.update({"destination": destination, "one_way": str(one_way).lower()})
            if departure_at:
                params["departure_at"] = departure_at
            if return_at:
                params["return_at"] = return_at
                try:
                    dep = date.fromisoformat(departure_at or "")
                    ret = date.fromisoformat(return_at)
                    diff = (ret - dep).days
                    if CFG.min_trip_days <= diff <= CFG.max_trip_days:
                        params["trip_duration"] = str(diff)
                except Exception:
                    pass
        if max_stops is not None and max_stops == 0:
            params["direct"] = "true"

        return params

    def search_prices(
        self,
        origin: str,
        destination: Optional[str] = None,
        departure_at: Optional[str] = None,
        return_at: Optional[str] = None,
        one_way: bool = True,
        currency: str = "PLN",
        limit: int = 100,
        max_age_h: int = 12,
        max_stops: Optional[int] = None,
    ) -> List[FlightOffer]:
        params = self._build_params(
            origin=origin,
            destination=destination,
            departure_at=departure_at,
            return_at=return_at,
            one_way=one_way,
            currency=currency,
            limit=limit,
            max_stops=max_stops,
        )

        endpoint = "/prices_for_dates" if destination else "/prices/latest"

        url = f"{self.base_url}{endpoint}"
        hdrs = {**BASE_HEADERS, "X-Access-Token": self.token}

        resp = requests.get(url, params=params, headers=hdrs, timeout=15)
        if resp.status_code != 200:
            raise AviasalesFetcherError(f"HTTP {resp.status_code} – {resp.text[:120]}")

        payload = resp.json()
        if not payload.get("success", True):
            raise AviasalesFetcherError("API response unsuccessful")

        offers: List[FlightOffer] = []
        for item in payload.get("data", []):
            if int(item.get("number_of_changes", item.get("stops", 0))) > CFG.max_stops:
                continue
            fetched_raw = item.get("found_at") or ""
            if fetched_raw and not self._within_age(fetched_raw, max_age_h):
                continue
            link = item.get("link")
            if not link:
                continue
            try:
                depart = date.fromisoformat(
                    item.get("depart_date") or item.get("departure_at", "")
                )
            except Exception:
                continue
            ret_s = item.get("return_date")
            ret_date = date.fromisoformat(ret_s) if ret_s else None
            if ret_date:
                trip_len = (ret_date - depart).days
                if trip_len < CFG.min_trip_days or trip_len > CFG.max_trip_days:
                    continue
            try:
                fetched = (
                    datetime.fromisoformat(fetched_raw.replace("Z", "+00:00"))
                    if fetched_raw
                    else datetime.now(timezone.utc)
                )
            except Exception:
                fetched = datetime.now(timezone.utc)
            total_ft = float(item.get("total_flight_time_h", 0.0))
            layover = float(item.get("max_layover_h", 0.0))
            if (total_ft and total_ft > CFG.max_layover_h) or (
                layover and layover > CFG.max_layover_h
            ):
                continue
            offers.append(
                FlightOffer(
                    origin=item.get("origin", origin),
                    destination=item.get("destination", destination or ""),
                    depart_date=depart,
                    return_date=ret_date,
                    price_pln=Decimal(str(item.get("price", 0.0))),
                    airline=item.get("airline", ""),
                    stops=int(item.get("stops", 0)),
                    total_flight_time_h=float(item.get("total_flight_time_h", 0.0)),
                    max_layover_h=float(item.get("max_layover_h", 0.0)),
                    deep_link=self._build_url(link),
                    fetched_at=fetched,
                )
            )
        return offers

    def save_offers(
        self,
        offers: Iterable[FlightOffer],
        backend: Literal["csv", "sqlite"],
        path: str = "aviasales_offers",
    ) -> None:
        if backend == "csv":
            fname = f"{path}.csv"
            is_new = not os.path.exists(fname)
            with open(fname, "a", newline="", encoding="utf-8") as fh:
                wr = csv.writer(fh)
                if is_new:
                    wr.writerow([
                        "origin",
                        "destination",
                        "depart",
                        "return_date",
                        "price_pln",
                        "airline",
                        "stops",
                        "total_flight_time_h",
                        "max_layover_h",
                        "deep_link",
                        "fetched_at",
                    ])
                wr.writerows([
                    (
                        o.origin,
                        o.destination,
                        o.depart_date,
                        o.return_date,
                        float(o.price_pln),
                        o.airline,
                        o.stops,
                        o.total_flight_time_h,
                        o.max_layover_h,
                        o.deep_link,
                        o.fetched_at.isoformat(),
                    )
                    for o in offers
                ])

        elif backend == "sqlite":
            conn = sqlite3.connect(f"{path}.db")
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(flights)")
            cols = [r[1] for r in cur.fetchall()]
            if not cols:
                cur.execute("""
                    CREATE TABLE flights(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        origin TEXT,
                        destination TEXT,
                        depart TEXT,
                        return_date TEXT,
                        price REAL,
                        price_per_km REAL,
                        score REAL,
                        notified_at TEXT,
                        airline TEXT,
                        deep_link TEXT,
                        fetched_at TEXT
                    )""")
            else:
                for name, typ in [
                    ("return_date", "TEXT"),
                    ("price_per_km", "REAL"),
                    ("score", "REAL"),
                    ("notified_at", "TEXT"),
                ]:
                    if name not in cols:
                        cur.execute(f"ALTER TABLE flights ADD COLUMN {name} {typ}")

            existing = set()
            cur.execute("SELECT origin, destination, depart, price, airline FROM flights")
            for row in cur.fetchall():
                existing.add(tuple(row))

            new_count = 0
            for o in offers:
                key = (o.origin, o.destination, o.depart_date, float(o.price_pln), o.airline)
                if key in existing:
                    continue
                try:
                    dist = distance_km(o.origin, o.destination)
                    ppkm = float(o.price_pln) / dist if dist else None
                except Exception:
                    ppkm = None
                cur.execute("""
                    INSERT INTO flights
                    (origin,destination,depart,return_date,price,price_per_km,
                     score,notified_at,airline,deep_link,fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    o.origin, o.destination, o.depart_date, o.return_date,
                    float(o.price_pln), ppkm, 0.0, None, o.airline, o.deep_link, o.fetched_at.isoformat()))
                new_count += 1

            conn.commit()
            conn.close()
            print(f"Saved {new_count} new offers ➔ sqlite")

        else:
            raise ValueError("backend must be 'csv' or 'sqlite'")


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch Aviasales flight offers")
    p.add_argument("origin")
    p.add_argument("--dest")
    p.add_argument("--depart")
    p.add_argument("--return-date")
    p.add_argument("--one-way", action="store_true")
    p.add_argument("--currency", default="PLN")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--hours", type=int, default=12)
    p.add_argument("--domain")
    p.add_argument("--plain", action="store_true", help="Print full links")
    p.add_argument("--save", choices=["csv", "sqlite"])
    args = p.parse_args()

    f = AviasalesFetcher(domain=args.domain or "https://www.aviasales.com")
    offers = f.search_prices(
        origin=args.origin,
        destination=args.dest,
        departure_at=args.depart,
        return_at=args.return_date,
        one_way=args.one_way,
        currency=args.currency,
        limit=args.limit,
        max_age_h=args.hours,
    )

    if not offers:
        print("No offers found")
        return

    rows = [{
        "origin": o.origin,
        "dest": o.destination,
        "depart": o.depart_date,
        "return": o.return_date,
        "price_pln": float(o.price_pln),
        "airline": o.airline,
        "link": o.deep_link if args.plain else o.deep_link[:50] + "…",
    } for o in offers]

    if args.plain:
        for r in rows:
            print(r)
    else:
        try:
            from tabulate import tabulate  # type: ignore
            print(tabulate(rows, headers="keys", tablefmt="github"))
        except ModuleNotFoundError:
            for r in rows:
                print(r)

    if args.save:
        f.save_offers(offers, args.save)


if __name__ == "__main__":
    main()

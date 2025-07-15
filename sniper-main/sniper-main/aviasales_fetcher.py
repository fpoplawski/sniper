from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Generator

import requests
from dotenv import load_dotenv

from config import Config
from models import FlightOffer

load_dotenv()
CFG = Config.from_json()


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
        self.token = token or CFG.tp_token or ""
        self.marker = marker or CFG.tp_marker or ""
        self.base_url = base_url.rstrip("/")
        self.domain = domain.rstrip("/")

    # ──────────────────────────────────────────────────────────

    def search_prices(
        self,
        origin: str,
        destination: str,
        departure_at: str | None = None,
        return_at: str | None = None,
        currency: str = "pln",
    ) -> Generator[FlightOffer, None, None]:
        """
        Zwraca generator ofert dla danej pary tras.
        Parametry *departure_at*, *return_at* – YYYY-MM lub YYYY-MM-DD.
        """

        url = (
            f"{self.base_url}/prices_for_dates?"
            f"origin={origin}&destination={destination}&currency={currency}"
            f"&token={self.token}"
        )
        if departure_at:
            url += f"&departure_at={departure_at}"
        if return_at:
            url += f"&return_at={return_at}"
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

        for item in data.get("data", []):
            yield self._to_offer(item)

    # ──────────────────────────────────────────────────────────

    def _to_offer(self, item: dict) -> FlightOffer:
        """Mapuje rekord JSON na obiekt FlightOffer."""
        price_pln = Decimal(str(item["price"]))

        dep_raw = item.get("departure_at") or item.get("depart_date")
        ret_raw = item.get("return_at") or item.get("return_date")

        depart = dt.date.fromisoformat(dep_raw[:10])
        return_dt = dt.date.fromisoformat(ret_raw[:10]) if ret_raw else None
        fetched_at = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

        deep_link = (
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


__all__ = ["AviasalesFetcher", "AviasalesFetcherError"]

from __future__ import annotations

from decimal import Decimal

from config import Config
from db import get_last_30d_avg
from aviasales_fetcher import FlightOffer


def is_steal(offer: FlightOffer, cfg: Config) -> bool:
    """Return ``True`` if *offer* price is a steal compared to recent average."""
    avg = get_last_30d_avg(offer.origin, offer.destination)
    if avg is None:
        return False  # za mało danych
    threshold = avg * Decimal(str(1 - cfg.steal_threshold))
    return offer.price_pln <= threshold


__all__ = ["is_steal"]

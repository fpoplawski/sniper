from __future__ import annotations

from decimal import Decimal
from typing import Any

from .db import get_last_30d_avg
from .models import FlightOffer


def is_steal(offer: FlightOffer, cfg: Any) -> bool:
    """Return ``True`` if *offer* price is a steal compared to recent average.

    Example:
        >>> cfg.steal_threshold = 0.20
        >>> is_steal(offer(price_pln=Decimal("800")), cfg)  # avg=1000
        True
    """

    avg = get_last_30d_avg(offer.origin, offer.destination)
    if avg is None or avg <= 0:
        return False
    threshold = avg * (Decimal("1") - Decimal(cfg.steal_threshold))
    return offer.price_pln <= threshold


__all__ = ["is_steal"]

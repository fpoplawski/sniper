"""Data models used throughout the project."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

@dataclass(slots=True)
class FlightOffer:
    origin: str
    destination: str
    depart_date: date
    return_date: Optional[date]
    price_pln: Decimal
    airline: str
    stops: int
    total_flight_time_h: Optional[float]
    max_layover_h: Optional[float]
    deep_link: str
    fetched_at: datetime
    alert_sent: bool = False

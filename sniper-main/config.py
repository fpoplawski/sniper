from __future__ import annotations

from dataclasses import dataclass

import json
import os
from typing import Any, Dict

# Supported configuration keys
CONFIG_FIELDS = [
    "origins",
    "destinations",
    "one_way",
    "min_trip_days",
    "max_trip_days",
    "max_price",
    "max_price_total",
    "top_n",
    "min_composite_score",
    "weight_price",
    "weight_price_per_km",
    "weight_baseline_diff",
    "weight_trip_duration",
    "passengers",
    "excluded_airlines",
    "steal_threshold",
    "max_stops",
    "max_layover_h",
    "poll_interval_h",
    "currency",
    "telegram_instant",
    "email_daily",
]


def load_config(path: str | None = None) -> "Config":
    """Return configuration loaded from *path* or ``config.json``."""
    cfg_path = path or os.path.join(os.getcwd(), "config.json")
    with open(cfg_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    defaults = {
        "passengers": 1,
        "max_price_total": None,
        "excluded_airlines": [],
        "min_composite_score": 0,
        "weight_price": 0.4,
        "weight_price_per_km": 0.3,
        "weight_baseline_diff": 0.2,
        "weight_trip_duration": 0.1,
        "steal_threshold": 0.20,
        "max_stops": 1,
        "max_layover_h": 6.0,
        "poll_interval_h": 6,
        "currency": "PLN",
        "telegram_instant": True,
        "email_daily": True,
    }

    cfg = {key: data.get(key, defaults.get(key)) for key in CONFIG_FIELDS}
    return Config(**{k: cfg[k] for k in Config.__dataclass_fields__})


__all__ = ["load_config", "Config"]


@dataclass(slots=True)
class Config:
    """Default configuration values."""

    min_trip_days: int = 5
    max_trip_days: int = 14
    steal_threshold: float = 0.20  # 20%
    max_stops: int = 1
    max_layover_h: float = 6.0
    poll_interval_h: int = 6  # co 6 godz.
    currency: str = "PLN"
    telegram_instant: bool = True
    email_daily: bool = True

    @classmethod
    def from_json(cls, path: str | None = None) -> "Config":
        return load_config(path)


__all__.append("Config")

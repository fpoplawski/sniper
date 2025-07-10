from __future__ import annotations

import json
import os
from typing import Any, Dict

# Expected keys in configuration file
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
]


def load_config(path: str | None = None) -> Dict[str, Any]:
    """Return configuration dictionary loaded from *path* or ``config.json``."""
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
    }

    cfg = {key: data.get(key, defaults.get(key)) for key in CONFIG_FIELDS}
    return cfg


__all__ = ["load_config"]

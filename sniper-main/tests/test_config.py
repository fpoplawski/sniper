import os
import sys
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import load_config


def test_load_config(tmp_path):
    cfg = {
        "origins": ["WAW"],
        "destinations": ["JFK", "LAX"],
        "one_way": False,
        "min_trip_days": 5,
        "max_trip_days": 15,
        "max_price": 1200,
        "top_n": 3,
        "min_composite_score": 55,
        "excluded_airlines": ["XX"],
        "extra": "ignored",
    }
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps(cfg))

    loaded = load_config(str(cfg_file))
    assert loaded == {
        "origins": ["WAW"],
        "destinations": ["JFK", "LAX"],
        "one_way": False,
        "min_trip_days": 5,
        "max_trip_days": 15,
        "max_price": 1200,
        "max_price_total": None,
        "top_n": 3,
        "min_composite_score": 55,
        "weight_price": 0.4,
        "weight_price_per_km": 0.3,
        "weight_baseline_diff": 0.2,
        "weight_trip_duration": 0.1,
        "passengers": 1,
        "excluded_airlines": ["XX"],
    }


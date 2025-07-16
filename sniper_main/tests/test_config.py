import os
import json

from sniper_main.config import Config


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

    loaded = Config.from_json(str(cfg_file))
    assert isinstance(loaded, Config)
    assert loaded.min_trip_days == 5
    assert loaded.max_trip_days == 15
    # defaults
    assert loaded.steal_threshold == 0.20
    assert loaded.max_stops == 1
    assert loaded.max_layover_h == 6.0
    assert loaded.poll_interval_h == 6
    assert loaded.currency == "PLN"
    assert loaded.telegram_instant is True
    assert loaded.email_daily is True


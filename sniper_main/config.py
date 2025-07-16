from __future__ import annotations
from dataclasses import dataclass, asdict
from decimal import Decimal
import json, os, pathlib
from typing import Any
_CFG_PATH = pathlib.Path(os.getenv("SNIPER_CONFIG", "config.json"))
from dotenv import load_dotenv
load_dotenv()          # ← bez find_dotenv()



@dataclass(slots=True)
class Config:
    # === parametry filtrowania / logiki STEAL ===
    origins: list[str] = None
    destinations: list[str] = None
    one_way: bool = False
    min_trip_days: int = 5
    max_trip_days: int = 14
    steal_threshold: float = 0.20
    max_stops: int = 1
    max_layover_h: float = 6.0
    max_total_time_h: float = 30.0
    currency: str = "PLN"
    poll_interval_h: int = 6

    # === powiadomienia ===
    telegram_instant: bool = True
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    email_daily: bool = True
    smtp_host: str | None = None
    smtp_port: int | None = 465
    smtp_user: str | None = None
    smtp_pass: str | None = None
    email_from: str | None = None
    email_to: str | None = None

    # === Aviasales ===
    tp_token: str | None = os.getenv("TP_TOKEN")   # ← musi tu być os.getenv
    tp_marker: str | None = os.getenv("TP_MARKER")
    domain: str = "https://www.aviasales.com"

    @classmethod
    def from_json(cls, path: str | pathlib.Path = _CFG_PATH) -> "Config":
        data: dict[str, Any] = {}
        if pathlib.Path(path).exists():
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        # dataclass pola → domyślne, nadpisz z JSON
        defaults = asdict(cls())        # type: ignore[arg-type]
        defaults.update(data)
        filtered = {k: defaults.get(k) for k in cls.__dataclass_fields__}
        return cls(**filtered)

__all__ = ["Config"]

import os
import sys
from dataclasses import dataclass
from decimal import Decimal
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import Config
import steal_engine


@dataclass(slots=True)
class Offer:
    origin: str
    destination: str
    price_pln: Decimal


def test_is_steal_true(monkeypatch):
    monkeypatch.setattr(steal_engine, "get_last_30d_avg", lambda o, d: Decimal("1000"))
    offer = Offer("WAW", "JFK", Decimal("750"))
    cfg = Config()
    assert steal_engine.is_steal(offer, cfg)


def test_is_steal_false_price(monkeypatch):
    monkeypatch.setattr(steal_engine, "get_last_30d_avg", lambda o, d: Decimal("100"))
    offer = Offer("WAW", "JFK", Decimal("90"))
    cfg = Config()
    assert not steal_engine.is_steal(offer, cfg)


def test_is_steal_no_data(monkeypatch):
    monkeypatch.setattr(steal_engine, "get_last_30d_avg", lambda o, d: None)
    offer = Offer("WAW", "JFK", Decimal("50"))
    cfg = Config()
    assert not steal_engine.is_steal(offer, cfg)


def test_is_steal_non_positive_avg(monkeypatch):
    monkeypatch.setattr(steal_engine, "get_last_30d_avg", lambda o, d: Decimal("0"))
    offer = Offer("WAW", "JFK", Decimal("10"))
    cfg = Config()
    assert not steal_engine.is_steal(offer, cfg)

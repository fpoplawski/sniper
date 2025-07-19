from sniper_main.config import get_settings, Settings


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "abc")
    monkeypatch.setenv("POLL_INTERVAL_H", "3")
    monkeypatch.setenv("AIRPORTS", '["WAW", "JFK"]')

    cfg = get_settings()
    assert isinstance(cfg, Settings)
    assert cfg.telegram_token == "abc"
    assert cfg.poll_interval_h == 3
    assert cfg.airports == ["WAW", "JFK"]

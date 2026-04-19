from pathlib import Path

from fastapi.testclient import TestClient

from tbank_trader.api.app import create_app
from tbank_trader.config import AppSettings


def test_health_and_dashboard(tmp_path: Path) -> None:
    database_path = tmp_path / "api-test.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        symbols=["SBER", "GAZP"],
        broker_mode="simulated",
    )
    client = TestClient(create_app(settings))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    dashboard = client.get("/api/dashboard")
    assert dashboard.status_code == 200
    payload = dashboard.json()
    assert payload["system"]["order_count"] == 0
    assert payload["system"]["strategy_mode"] == "portfolio_momentum"
    assert payload["system"]["strategy_portfolio_live_enabled"] is True
    assert payload["shadow_portfolio"]["enabled"] is False
    assert payload["portfolio_live"]["enabled"] is True
    assert payload["shadow_trades"] == []
    assert payload["shadow_rebalances"] == []
    assert payload["portfolio_trades"] == []
    assert payload["portfolio_rebalances"] == []
    assert payload["system"]["strategy_profile"] == "balanced"
    assert payload["system"]["share_target_order_notional_rub"] == 5000.0
    assert len(payload["prices"]) == 2


def test_pause_and_resume(tmp_path: Path) -> None:
    database_path = tmp_path / "pause-test.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
    )
    client = TestClient(create_app(settings))

    paused = client.post("/api/system/pause")
    assert paused.status_code == 200
    assert paused.json()["status"] == "paused"

    system = client.get("/api/system")
    assert system.status_code == 200
    assert system.json()["paused"] is True

    resumed = client.post("/api/system/resume")
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "running"

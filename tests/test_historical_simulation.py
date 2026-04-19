from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from tbank_trader.api.app import create_app
from tbank_trader.config import AppSettings
from tbank_trader.services.benchmark_regime import RegimeSnapshot
import tbank_trader.services.historical_simulation as historical_simulation
from tbank_trader.services.historical_simulation import _build_daily_price_bars
from tbank_trader.services.historical_simulation import run_historical_portfolio_simulation
from tbank_trader.services.instrument_catalog import sync_instrument_catalog
from tbank_trader.services.tbank_client import CatalogInstrument
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    build_dashboard,
    serialize_latest_historical_simulation,
    upsert_historical_candles,
)


class FakeSimulationCatalogSource:
    def list_all_catalog_instruments(self) -> list[CatalogInstrument]:
        return [
            CatalogInstrument(
                instrument_uid="uid-aaa",
                figi="figi-aaa",
                ticker="AAA",
                class_code="TQBR",
                instrument_type="share",
                name="AAA",
                lot=1,
                currency="rub",
                exchange="MOEX",
                country_of_risk="RU",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=True,
                for_qual_investor_flag=False,
                weekend_flag=False,
                otc_flag=False,
                active_flag=True,
            ),
            CatalogInstrument(
                instrument_uid="uid-bbb",
                figi="figi-bbb",
                ticker="BBB",
                class_code="TQBR",
                instrument_type="share",
                name="BBB",
                lot=1,
                currency="rub",
                exchange="MOEX",
                country_of_risk="RU",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=True,
                for_qual_investor_flag=False,
                weekend_flag=False,
                otc_flag=False,
                active_flag=True,
            ),
            CatalogInstrument(
                instrument_uid="uid-ccc",
                figi="figi-ccc",
                ticker="CCC",
                class_code="TQBR",
                instrument_type="share",
                name="CCC",
                lot=1,
                currency="rub",
                exchange="MOEX",
                country_of_risk="RU",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=True,
                for_qual_investor_flag=False,
                weekend_flag=False,
                otc_flag=False,
                active_flag=True,
            ),
        ]


def build_test_session_factory(database_path: Path):
    database_url = f"sqlite:///{database_path}"
    engine = build_engine(database_url)
    init_database(engine)
    return build_session_factory(database_url)


def seed_historical_candles(session) -> None:
    base_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    trajectories = {
        "uid-aaa": [100.0 + step * 1.2 for step in range(32)],
        "uid-bbb": [100.0 + step * 0.6 for step in range(32)],
        "uid-ccc": [120.0 - step * 0.4 for step in range(32)],
    }

    candles: list[dict[str, object]] = []
    for instrument_uid, closes in trajectories.items():
        for idx, close_price in enumerate(closes):
            candle_time = base_time + timedelta(days=idx)
            candles.append(
                {
                    "instrument_uid": instrument_uid,
                    "interval": "CANDLE_INTERVAL_5_MIN",
                    "candle_time": candle_time,
                    "open_price": close_price - 0.2,
                    "high_price": close_price + 0.3,
                    "low_price": close_price - 0.4,
                    "close_price": close_price,
                    "volume": 1000 + idx,
                    "turnover_rub": (1000 + idx) * close_price,
                    "is_complete": True,
                }
            )
    upsert_historical_candles(session, candles)
    session.commit()


def test_build_daily_price_bars_separates_open_and_close_roles() -> None:
    bars = _build_daily_price_bars(
        {
            "AAA": [
                (datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc), 99.0, 100.0),
                (datetime(2026, 1, 1, 18, 0, tzinfo=timezone.utc), 101.0, 102.0),
                (datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc), 103.0, 104.0),
                (datetime(2026, 1, 2, 18, 0, tzinfo=timezone.utc), 105.0, 106.0),
            ]
        }
    )

    assert len(bars["AAA"]) == 2
    assert bars["AAA"][0].open_price == 99.0
    assert bars["AAA"][0].close_price == 102.0
    assert bars["AAA"][1].open_price == 103.0
    assert bars["AAA"][1].close_price == 106.0


def test_historical_simulation_stores_run_and_rebalances(tmp_path: Path) -> None:
    database_path = tmp_path / "simulation.db"
    session_factory = build_test_session_factory(database_path)
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        symbols=["AAA", "BBB", "CCC"],
        portfolio_regime_symbol="AAA",
        portfolio_momentum_periods=[3, 5, 8],
        portfolio_regime_ma_window=5,
        portfolio_top_percentile=80,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
        portfolio_rebalance_frequency="W",
        historical_regime_mode="local_ma",
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeSimulationCatalogSource())
        seed_historical_candles(session)
        summary = run_historical_portfolio_simulation(session, settings=settings)
        payload = build_dashboard(session)
        latest_simulation = serialize_latest_historical_simulation(session)

    assert summary.status == "completed"
    assert summary.instruments_considered == 3
    assert summary.instruments_with_history == 3
    assert summary.completed_rebalances >= 1
    assert summary.executed_actions >= 1
    assert summary.final_equity_rub > 0
    assert latest_simulation is not None
    assert latest_simulation["status"] == "completed"
    assert "price_roles=ranking_daily_close_prev_day_execution_daily_open_mark_daily_close" in summary.note
    assert payload["historical_simulation"]["id"] == summary.run_id
    assert payload["system"]["simulation_last_run_id"] == summary.run_id
    assert payload["system"]["simulation_last_rebalances"] >= 1


def test_latest_simulation_api_returns_run_and_rebalances(tmp_path: Path) -> None:
    database_path = tmp_path / "simulation-api.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        symbols=["AAA", "BBB", "CCC"],
        portfolio_regime_symbol="AAA",
        portfolio_momentum_periods=[3, 5, 8],
        portfolio_regime_ma_window=5,
        portfolio_top_percentile=80,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
        portfolio_rebalance_frequency="W",
        historical_regime_mode="local_ma",
    )
    client = TestClient(create_app(settings))

    with client.app.state.session_factory() as session:
        sync_instrument_catalog(session, settings=settings, source=FakeSimulationCatalogSource())
        seed_historical_candles(session)
        run_historical_portfolio_simulation(session, settings=settings)

    response = client.get("/api/simulation/latest")
    assert response.status_code == 200
    payload = response.json()
    assert payload["run"] is not None
    assert payload["run"]["status"] == "completed"
    assert len(payload["rebalances"]) >= 1


def test_historical_simulation_can_use_external_spx_vix_regime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "simulation-external-regime.db"
    session_factory = build_test_session_factory(database_path)
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        symbols=["AAA", "BBB", "CCC"],
        portfolio_regime_symbol="AAA",
        portfolio_momentum_periods=[3, 5, 8],
        portfolio_regime_ma_window=5,
        portfolio_top_percentile=80,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
        portfolio_rebalance_frequency="W",
        historical_regime_mode="spx_vix",
    )

    def fake_regime_loader(**_: object) -> dict[datetime.date, RegimeSnapshot]:
        base_date = datetime(2026, 1, 1).date()
        return {
            base_date + timedelta(days=offset): RegimeSnapshot(
                is_on=True,
                reason="spx_vix_on:fake",
            )
            for offset in range(40)
        }

    monkeypatch.setattr(
        historical_simulation,
        "load_spx_vix_regime_history",
        fake_regime_loader,
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeSimulationCatalogSource())
        seed_historical_candles(session)
        summary = run_historical_portfolio_simulation(session, settings=settings)

    assert summary.status == "completed"
    assert summary.completed_rebalances >= 1
    assert summary.executed_actions >= 1
    assert "regime_mode=spx_vix" in summary.note
    assert "price_roles=ranking_daily_close_prev_day_execution_daily_open_mark_daily_close" in summary.note


def test_historical_simulation_falls_back_to_local_regime_when_external_load_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "simulation-external-regime-fallback.db"
    session_factory = build_test_session_factory(database_path)
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        symbols=["AAA", "BBB", "CCC"],
        portfolio_regime_symbol="AAA",
        portfolio_momentum_periods=[3, 5, 8],
        portfolio_regime_ma_window=5,
        portfolio_top_percentile=80,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
        portfolio_rebalance_frequency="W",
        historical_regime_mode="spx_vix",
    )

    def fail_regime_loader(**_: object) -> dict[datetime.date, RegimeSnapshot]:
        raise RuntimeError("benchmark_provider_failed")

    monkeypatch.setattr(
        historical_simulation,
        "load_spx_vix_regime_history",
        fail_regime_loader,
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeSimulationCatalogSource())
        seed_historical_candles(session)
        summary = run_historical_portfolio_simulation(session, settings=settings)

    assert summary.status == "completed"
    assert summary.completed_rebalances >= 1
    assert summary.executed_actions >= 1
    assert "regime_mode=local_ma_fallback_after_spx_vix_load_failure" in summary.note
    assert "price_roles=ranking_daily_close_prev_day_execution_daily_open_mark_daily_close" in summary.note

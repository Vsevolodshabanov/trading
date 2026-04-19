from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from tbank_trader.api.app import create_app
from tbank_trader.config import AppSettings
from tbank_trader.services.historical_data import (
    backfill_historical_candles,
    backfill_historical_candles_batch,
)
from tbank_trader.services.instrument_catalog import sync_instrument_catalog
from tbank_trader.services.tbank_client import CatalogInstrument
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import bootstrap_defaults, build_dashboard


class FakeCatalogSource:
    def list_all_catalog_instruments(self) -> list[CatalogInstrument]:
        return [
            CatalogInstrument(
                instrument_uid="uid-sber",
                figi="BBG004730N88",
                ticker="SBER",
                class_code="TQBR",
                instrument_type="share",
                name="Sberbank",
                lot=10,
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
                instrument_uid="uid-qual",
                figi="BBG000QUAL1",
                ticker="QUAL",
                class_code="TQBR",
                instrument_type="share",
                name="Qual Only",
                lot=1,
                currency="rub",
                exchange="MOEX",
                country_of_risk="RU",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=False,
                for_qual_investor_flag=True,
                weekend_flag=False,
                otc_flag=False,
                active_flag=True,
            ),
        ]


class FakeHistoricalSource:
    def get_candles(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
        interval: str,
        limit: int,
    ) -> list[dict]:
        base = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        if instrument_id != "uid-sber":
            return []
        return [
            {
                "time": (base + timedelta(minutes=5 * idx)).isoformat().replace("+00:00", "Z"),
                "open": {"units": "300", "nano": idx * 1000000},
                "high": {"units": "301", "nano": idx * 1000000},
                "low": {"units": "299", "nano": idx * 1000000},
                "close": {"units": "300", "nano": idx * 2000000},
                "volume": 1000 + idx,
                "isComplete": True,
            }
            for idx in range(3)
        ]


class FakeBatchHistoricalSource:
    def get_candles(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
        interval: str,
        limit: int,
    ) -> list[dict]:
        base = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        series_map = {
            "uid-sber": {"bars": 6, "close": 300.0, "volume": 2000},
            "uid-qual": {"bars": 6, "close": 10.0, "volume": 10},
        }
        spec = series_map.get(instrument_id)
        if spec is None:
            return []
        return [
            {
                "time": (base + timedelta(minutes=5 * idx)).isoformat().replace("+00:00", "Z"),
                "open": {"units": str(int(spec["close"])), "nano": 0},
                "high": {"units": str(int(spec["close"] + 1)), "nano": 0},
                "low": {"units": str(int(spec["close"] - 1)), "nano": 0},
                "close": {"units": str(int(spec["close"])), "nano": 0},
                "volume": spec["volume"] + idx,
                "isComplete": True,
            }
            for idx in range(spec["bars"])
        ]


def build_test_session_factory(database_path: Path):
    database_url = f"sqlite:///{database_path}"
    engine = build_engine(database_url)
    init_database(engine)
    return build_session_factory(database_url)


def test_historical_backfill_stores_candles_and_updates_dashboard(tmp_path: Path) -> None:
    session_factory = build_test_session_factory(tmp_path / "history.db")
    settings = AppSettings(
        database_url=f"sqlite:///{tmp_path / 'history.db'}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeCatalogSource())
        summary = backfill_historical_candles(
            session,
            settings=settings,
            source=FakeHistoricalSource(),
        )
        payload = build_dashboard(session)

    assert summary.eligible_instruments == 1
    assert summary.instruments_attempted == 1
    assert summary.instruments_with_data == 1
    assert summary.candles_written == 3
    assert summary.total_candles == 3
    assert summary.history_ready_instruments == 0
    assert payload["system"]["historical_candle_count"] == 3
    assert payload["system"]["historical_covered_instruments"] == 1
    assert payload["system"]["history_ready_instruments"] == 0
    assert payload["system"]["historical_last_candle_at"] is not None


def test_universe_history_api_returns_backfill_status(tmp_path: Path) -> None:
    database_path = tmp_path / "history-api.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
    )
    client = TestClient(create_app(settings))

    with client.app.state.session_factory() as session:
        sync_instrument_catalog(session, settings=settings, source=FakeCatalogSource())
        backfill_historical_candles(
            session,
            settings=settings,
            source=FakeHistoricalSource(),
        )

    response = client.get("/api/universe/history")
    assert response.status_code == 200
    payload = response.json()
    assert payload["candles"] == 3
    assert payload["covered_instruments"] == 1
    assert payload["history_ready_instruments"] == 0
    assert payload["latest_candle_at"] is not None


def test_historical_backfill_batch_tracks_cursor_and_history_ready(tmp_path: Path) -> None:
    database_path = tmp_path / "history-batch.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
        historical_backfill_batch_size=1,
        historical_min_candle_count=5,
        universe_min_median_turnover_rub=100_000.0,
    )
    session_factory = build_test_session_factory(database_path)

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeCatalogSource())
        first = backfill_historical_candles_batch(
            session,
            settings=settings,
            source=FakeBatchHistoricalSource(),
        )
        second = backfill_historical_candles_batch(
            session,
            settings=settings,
            source=FakeBatchHistoricalSource(),
        )
        dashboard = build_dashboard(session)

    assert first.instruments_attempted == 1
    assert first.next_cursor == 0
    assert first.cycle_completed is True
    assert first.history_ready_instruments == 1
    assert second.instruments_attempted == 1
    assert second.history_ready_instruments == 1
    assert dashboard["system"]["history_ready_instruments"] == 1
    assert dashboard["system"]["history_backfill_last_batch_size"] == 1
    assert dashboard["system"]["history_backfill_cycle_completed"] is True

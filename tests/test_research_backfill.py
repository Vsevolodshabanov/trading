from datetime import datetime, timezone
from pathlib import Path

from tbank_trader.config import AppSettings
from tbank_trader.services.instrument_catalog import sync_instrument_catalog
from tbank_trader.services.research_backfill import backfill_ru_share_research_data
from tbank_trader.services.tbank_client import CatalogInstrument
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    count_dividend_events,
    count_historical_candles,
    load_dividend_events_for_instruments,
    load_instrument_research_status_for_instruments,
)


class FakeResearchCatalogSource:
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
                instrument_uid="uid-aapl",
                figi="BBG000B9XRY4",
                ticker="AAPL",
                class_code="SPBXM",
                instrument_type="share",
                name="Apple",
                lot=1,
                currency="usd",
                exchange="SPBX",
                country_of_risk="US",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=False,
                for_qual_investor_flag=False,
                weekend_flag=False,
                otc_flag=False,
                active_flag=True,
            ),
        ]


class FakeResearchSource:
    def get_candles(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
        interval: str,
        limit: int,
    ) -> list[dict]:
        if instrument_id != "uid-sber":
            return []
        return [
            {
                "time": "2022-01-03T00:00:00Z",
                "open": {"units": "250", "nano": 0},
                "high": {"units": "255", "nano": 0},
                "low": {"units": "249", "nano": 0},
                "close": {"units": "254", "nano": 0},
                "volume": 1000000,
                "isComplete": True,
            },
            {
                "time": "2022-01-04T00:00:00Z",
                "open": {"units": "254", "nano": 0},
                "high": {"units": "258", "nano": 0},
                "low": {"units": "253", "nano": 0},
                "close": {"units": "257", "nano": 0},
                "volume": 1200000,
                "isComplete": True,
            },
        ]

    def get_dividends(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
    ) -> list[dict]:
        if instrument_id != "uid-sber":
            return []
        return [
            {
                "dividendNet": {"currency": "rub", "units": "25", "nano": 500000000},
                "paymentDate": "2022-07-15T00:00:00Z",
                "declaredDate": "2022-05-24T00:00:00Z",
                "lastBuyDate": "2022-07-08T00:00:00Z",
                "dividendType": "Regular Cash",
                "recordDate": "2022-07-11T00:00:00Z",
                "regularity": "Annual",
                "closePrice": {"currency": "rub", "units": "240", "nano": 0},
                "yieldValue": {"units": "10", "nano": 0},
                "createdAt": "2022-05-24T12:00:00Z",
            }
        ]

    def get_instrument_by_uid(self, instrument_uid: str) -> dict:
        if instrument_uid != "uid-sber":
            return {}
        return {
            "tradingStatus": "SECURITY_TRADING_STATUS_NORMAL_TRADING",
            "buyAvailableFlag": True,
            "sellAvailableFlag": True,
            "apiTradeAvailableFlag": True,
            "otcFlag": False,
            "blockedTcaFlag": False,
            "first1MinCandleDate": "2018-01-01T00:00:00Z",
            "first1DayCandleDate": "2007-01-01T00:00:00Z",
        }


def build_test_session_factory(database_path: Path):
    database_url = f"sqlite:///{database_path}"
    engine = build_engine(database_url)
    init_database(engine)
    return build_session_factory(database_url)


def test_research_backfill_persists_ru_share_daily_data_dividends_and_status(tmp_path: Path) -> None:
    database_path = tmp_path / "research.db"
    session_factory = build_test_session_factory(database_path)
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        historical_min_candle_count=2,
        universe_min_median_turnover_rub=100_000.0,
    )
    export_dir = tmp_path / "export"

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        sync_instrument_catalog(session, settings=settings, source=FakeResearchCatalogSource())
        summary = backfill_ru_share_research_data(
            session,
            settings=settings,
            source=FakeResearchSource(),
            start_at=datetime(2022, 1, 1, tzinfo=timezone.utc),
            end_at=datetime(2022, 12, 31, tzinfo=timezone.utc),
            interval="CANDLE_INTERVAL_DAY",
            request_pause_seconds=0.0,
            export_dir=export_dir,
        )
        dividends = load_dividend_events_for_instruments(session, instrument_uids=["uid-sber"])
        statuses = load_instrument_research_status_for_instruments(session, instrument_uids=["uid-sber"])
    with session_factory() as session:
        total_candles = count_historical_candles(session, interval="CANDLE_INTERVAL_DAY")
        total_dividends = count_dividend_events(session)

    assert summary.russian_shares == 1
    assert summary.candles_written == 2
    assert summary.dividend_events_written == 1
    assert summary.status_snapshots_written == 1
    assert total_candles == 2
    assert total_dividends == 1
    assert len(dividends) == 1
    assert dividends[0]["currency"] == "rub"
    assert len(statuses) == 1
    assert statuses[0]["trading_status"] == "SECURITY_TRADING_STATUS_NORMAL_TRADING"
    assert (export_dir / "instruments.csv").exists()
    assert (export_dir / "daily_candles.csv").exists()
    assert (export_dir / "dividends.csv").exists()
    assert (export_dir / "instrument_status.csv").exists()
    assert (export_dir / "summary.json").exists()

from pathlib import Path

from fastapi.testclient import TestClient

from tbank_trader.api.app import create_app
from tbank_trader.config import AppSettings
from tbank_trader.services.instrument_catalog import sync_instrument_catalog
from tbank_trader.services.tbank_client import CatalogInstrument
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import bootstrap_defaults, build_dashboard, serialize_instrument_universe


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
            CatalogInstrument(
                instrument_uid="uid-otc",
                figi="BBG000OTC01",
                ticker="OTCX",
                class_code="SPBXM",
                instrument_type="share",
                name="OTC Asset",
                lot=1,
                currency="usd",
                exchange="SPB",
                country_of_risk="US",
                buy_available_flag=True,
                sell_available_flag=True,
                api_trade_available_flag=True,
                for_iis_flag=False,
                for_qual_investor_flag=False,
                weekend_flag=False,
                otc_flag=True,
                active_flag=True,
            ),
        ]


def build_test_session_factory(database_path: Path):
    database_url = f"sqlite:///{database_path}"
    engine = build_engine(database_url)
    init_database(engine)
    return build_session_factory(database_url)


def test_instrument_catalog_sync_builds_catalog_and_eligibility(tmp_path: Path) -> None:
    session_factory = build_test_session_factory(tmp_path / "catalog.db")
    settings = AppSettings(
        database_url=f"sqlite:///{tmp_path / 'catalog.db'}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        summary = sync_instrument_catalog(
            session,
            settings=settings,
            source=FakeCatalogSource(),
        )
        payload = build_dashboard(session)
        universe = serialize_instrument_universe(session, limit=10)

    assert summary.catalog_count == 3
    assert summary.eligible_count == 1
    assert summary.inserted_or_updated == 3
    assert payload["system"]["universe_catalog_size"] == 3
    assert payload["system"]["universe_eligible_size"] == 1
    assert universe[0]["eligible"] in (True, False)
    reasons_by_ticker = {row["ticker"]: row["reason_codes"] for row in universe}
    assert reasons_by_ticker["QUAL"] == "qual_only"
    assert reasons_by_ticker["OTCX"] == "otc"
    assert reasons_by_ticker["SBER"] == ""


def test_universe_catalog_api_returns_synced_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog-api.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        symbols=["SBER"],
        broker_mode="simulated",
    )
    client = TestClient(create_app(settings))

    with client.app.state.session_factory() as session:
        sync_instrument_catalog(
            session,
            settings=settings,
            source=FakeCatalogSource(),
        )

    response = client.get("/api/universe/catalog?limit=10")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 3
    assert {row["ticker"] for row in payload} == {"SBER", "QUAL", "OTCX"}

    eligible_only = client.get("/api/universe/catalog?limit=10&eligible_only=true")
    assert eligible_only.status_code == 200
    eligible_rows = eligible_only.json()
    assert len(eligible_rows) == 1
    assert eligible_rows[0]["ticker"] == "SBER"

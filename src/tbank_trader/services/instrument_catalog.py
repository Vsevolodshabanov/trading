from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.orm import Session

from tbank_trader.config import AppSettings
from tbank_trader.services.tbank_client import CatalogInstrument, TBankRestClient
from tbank_trader.storage.repository import (
    count_eligible_instruments,
    count_instrument_catalog,
    upsert_instrument_catalog,
    upsert_instrument_eligibility,
)


@dataclass(slots=True)
class InstrumentEligibilitySnapshot:
    instrument_uid: str
    eligible: bool
    reason_codes: str


@dataclass(slots=True)
class InstrumentCatalogSyncSummary:
    catalog_count: int
    eligible_count: int
    inserted_or_updated: int


class CatalogSource(Protocol):
    def list_all_catalog_instruments(self) -> list[CatalogInstrument]:
        raise NotImplementedError


def build_instrument_eligibility(
    instrument: CatalogInstrument,
    settings: AppSettings,
) -> InstrumentEligibilitySnapshot:
    reasons: list[str] = []

    if instrument.lot <= 0:
        reasons.append("invalid_lot")
    if not instrument.active_flag:
        reasons.append("inactive")
    if not instrument.api_trade_available_flag:
        reasons.append("api_trade_unavailable")
    if not instrument.buy_available_flag and not instrument.sell_available_flag:
        reasons.append("buy_sell_unavailable")
    if instrument.otc_flag:
        reasons.append("otc")
    if settings.universe_exclude_qual_only and instrument.for_qual_investor_flag:
        reasons.append("qual_only")

    return InstrumentEligibilitySnapshot(
        instrument_uid=instrument.instrument_uid,
        eligible=len(reasons) == 0,
        reason_codes=",".join(reasons),
    )


def sync_instrument_catalog(
    session: Session,
    *,
    settings: AppSettings,
    source: CatalogSource,
) -> InstrumentCatalogSyncSummary:
    instruments = source.list_all_catalog_instruments()
    eligibility = [
        build_instrument_eligibility(instrument, settings)
        for instrument in instruments
    ]

    upsert_instrument_catalog(session, instruments)
    upsert_instrument_eligibility(session, eligibility)
    session.commit()

    return InstrumentCatalogSyncSummary(
        catalog_count=count_instrument_catalog(session),
        eligible_count=count_eligible_instruments(session),
        inserted_or_updated=len(instruments),
    )


def main() -> None:
    from tbank_trader.config import get_settings
    from tbank_trader.storage.db import build_engine, build_session_factory, init_database

    settings = get_settings()
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)

    client = TBankRestClient(settings)
    try:
        with session_factory() as session:
            summary = sync_instrument_catalog(
                session,
                settings=settings,
                source=client,
            )
    finally:
        client.close()

    print(
        f"Instrument catalog synced: total={summary.catalog_count} "
        f"eligible={summary.eligible_count} updated={summary.inserted_or_updated}"
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Protocol

from sqlalchemy.orm import Session

from tbank_trader.config import AppSettings
from tbank_trader.services.tbank_client import quotation_to_float
from tbank_trader.storage.repository import (
    count_eligible_instruments,
    count_historical_candles,
    count_history_ready_instruments,
    count_instruments_with_historical_candles,
    get_eligible_instruments_for_backfill,
    get_latest_historical_candle_at,
    get_state,
    load_historical_candles_for_instruments,
    set_state,
    upsert_historical_candles,
    upsert_instrument_history_quality,
)


@dataclass(slots=True)
class HistoricalBackfillSummary:
    eligible_instruments: int
    instruments_attempted: int
    instruments_with_data: int
    candles_written: int
    total_candles: int
    history_ready_instruments: int
    next_cursor: int = 0
    cycle_completed: bool = False


@dataclass(slots=True)
class HistoryQualitySummary:
    tracked_instruments: int
    history_ready_instruments: int


class HistoricalCandleSource(Protocol):
    def get_candles(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
        interval: str,
        limit: int,
    ) -> list[dict]:
        raise NotImplementedError


def normalize_candle_payload(
    *,
    instrument_uid: str,
    interval: str,
    candle: dict,
) -> dict[str, object] | None:
    candle_time_raw = candle.get("time")
    if not candle_time_raw:
        return None
    candle_time = datetime.fromisoformat(str(candle_time_raw).replace("Z", "+00:00"))
    close_price = quotation_to_float(candle.get("close"))
    volume = float(candle.get("volume", 0) or 0)
    return {
        "instrument_uid": instrument_uid,
        "interval": interval,
        "candle_time": candle_time,
        "open_price": quotation_to_float(candle.get("open")),
        "high_price": quotation_to_float(candle.get("high")),
        "low_price": quotation_to_float(candle.get("low")),
        "close_price": close_price,
        "volume": volume,
        "turnover_rub": volume * close_price,
        "is_complete": bool(candle.get("isComplete", False)),
    }


def build_history_quality_snapshots(
    session: Session,
    *,
    settings: AppSettings,
) -> list[dict[str, object]]:
    instruments = get_eligible_instruments_for_backfill(session)
    if not instruments:
        return []

    instrument_uids = [str(instrument["instrument_uid"]) for instrument in instruments]
    candle_rows = load_historical_candles_for_instruments(
        session,
        instrument_uids=instrument_uids,
        interval=settings.strategy_candle_interval,
    )
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in candle_rows:
        grouped.setdefault(str(row["instrument_uid"]), []).append(row)

    snapshots: list[dict[str, object]] = []
    for instrument in instruments:
        instrument_uid = str(instrument["instrument_uid"])
        rows = grouped.get(instrument_uid, [])
        completed_rows = [row for row in rows if bool(row["is_complete"])]
        turnovers = [float(row["turnover_rub"]) for row in completed_rows]
        completed_candles = len(completed_rows)
        median_turnover_rub = float(median(turnovers)) if turnovers else 0.0
        latest_candle_at = completed_rows[-1]["candle_time"] if completed_rows else None

        reasons: list[str] = []
        if completed_candles < settings.historical_min_candle_count:
            reasons.append("history_short")
        if median_turnover_rub < settings.universe_min_median_turnover_rub:
            reasons.append("low_turnover")

        snapshots.append(
            {
                "instrument_uid": instrument_uid,
                "interval": settings.strategy_candle_interval,
                "completed_candles": completed_candles,
                "median_turnover_rub": median_turnover_rub,
                "latest_candle_at": latest_candle_at,
                "history_ready": len(reasons) == 0,
                "reason_codes": ",".join(reasons),
            }
        )
    return snapshots


def refresh_history_quality(
    session: Session,
    *,
    settings: AppSettings,
) -> HistoryQualitySummary:
    snapshots = build_history_quality_snapshots(session, settings=settings)
    upsert_instrument_history_quality(session, snapshots)
    session.commit()
    return HistoryQualitySummary(
        tracked_instruments=len(snapshots),
        history_ready_instruments=count_history_ready_instruments(
            session,
            interval=settings.strategy_candle_interval,
        ),
    )


def _backfill_selected_instruments(
    session: Session,
    *,
    settings: AppSettings,
    source: HistoricalCandleSource,
    instruments: list[dict[str, object]],
) -> HistoricalBackfillSummary:
    now = datetime.now(timezone.utc)
    from_ = now - timedelta(days=settings.historical_backfill_days)
    total_written = 0
    instruments_with_data = 0

    for instrument in instruments:
        raw_candles = source.get_candles(
            instrument_id=str(instrument["instrument_uid"]),
            from_=from_,
            to=now,
            interval=settings.strategy_candle_interval,
            limit=settings.historical_backfill_limit_per_request,
        )
        normalized = [
            row
            for row in (
                normalize_candle_payload(
                    instrument_uid=str(instrument["instrument_uid"]),
                    interval=settings.strategy_candle_interval,
                    candle=candle,
                )
                for candle in raw_candles
            )
            if row is not None
        ]
        if normalized:
            instruments_with_data += 1
            total_written += len(normalized)
            upsert_historical_candles(session, normalized)

    session.commit()
    quality_summary = refresh_history_quality(session, settings=settings)

    return HistoricalBackfillSummary(
        eligible_instruments=count_eligible_instruments(session),
        instruments_attempted=len(instruments),
        instruments_with_data=instruments_with_data,
        candles_written=total_written,
        total_candles=count_historical_candles(session, interval=settings.strategy_candle_interval),
        history_ready_instruments=quality_summary.history_ready_instruments,
    )


def backfill_historical_candles(
    session: Session,
    *,
    settings: AppSettings,
    source: HistoricalCandleSource,
    instrument_limit: int | None = None,
) -> HistoricalBackfillSummary:
    eligible_instruments = get_eligible_instruments_for_backfill(session)
    if instrument_limit is not None:
        eligible_instruments = eligible_instruments[:instrument_limit]
    return _backfill_selected_instruments(
        session,
        settings=settings,
        source=source,
        instruments=eligible_instruments,
    )


def backfill_historical_candles_batch(
    session: Session,
    *,
    settings: AppSettings,
    source: HistoricalCandleSource,
    batch_size: int | None = None,
) -> HistoricalBackfillSummary:
    eligible_instruments = get_eligible_instruments_for_backfill(session)
    total_instruments = len(eligible_instruments)
    if total_instruments == 0:
        summary = HistoricalBackfillSummary(
            eligible_instruments=0,
            instruments_attempted=0,
            instruments_with_data=0,
            candles_written=0,
            total_candles=0,
            history_ready_instruments=0,
            next_cursor=0,
            cycle_completed=False,
        )
        set_state(session, "history.backfill_cursor", "0")
        set_state(session, "history.backfill_last_batch_at", datetime.now(timezone.utc).isoformat())
        set_state(session, "history.backfill_last_batch_size", "0")
        set_state(session, "history.backfill_last_batch_written", "0")
        set_state(session, "history.backfill_cycle_completed", "false")
        session.commit()
        return summary

    resolved_batch_size = max(batch_size or settings.historical_backfill_batch_size, 1)
    cursor = int(get_state(session, "history.backfill_cursor", "0") or "0")

    if cursor >= total_instruments:
        cursor = 0

    batch = eligible_instruments[cursor : cursor + resolved_batch_size]
    next_cursor = cursor + len(batch)
    cycle_completed = next_cursor >= total_instruments
    if cycle_completed:
        next_cursor = 0

    summary = _backfill_selected_instruments(
        session,
        settings=settings,
        source=source,
        instruments=batch,
    )
    summary.next_cursor = next_cursor
    summary.cycle_completed = cycle_completed

    set_state(session, "history.backfill_cursor", str(next_cursor))
    set_state(session, "history.backfill_last_batch_at", datetime.now(timezone.utc).isoformat())
    set_state(session, "history.backfill_last_batch_size", str(len(batch)))
    set_state(session, "history.backfill_last_batch_written", str(summary.candles_written))
    set_state(session, "history.backfill_cycle_completed", str(cycle_completed).lower())
    session.commit()
    return summary


def main() -> None:
    from tbank_trader.config import get_settings
    from tbank_trader.services.tbank_client import TBankRestClient
    from tbank_trader.storage.db import build_engine, build_session_factory, init_database

    settings = get_settings()
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)

    client = TBankRestClient(settings)
    try:
        with session_factory() as session:
            summary = backfill_historical_candles_batch(
                session,
                settings=settings,
                source=client,
            )
            latest_candle = get_latest_historical_candle_at(
                session,
                interval=settings.strategy_candle_interval,
            )
            covered = count_instruments_with_historical_candles(
                session,
                interval=settings.strategy_candle_interval,
            )
    finally:
        client.close()

    print(
        f"Historical candles synced: eligible={summary.eligible_instruments} "
        f"attempted={summary.instruments_attempted} instruments_with_data={summary.instruments_with_data} "
        f"written={summary.candles_written} total={summary.total_candles} covered={covered} "
        f"history_ready={summary.history_ready_instruments} next_cursor={summary.next_cursor} "
        f"cycle_completed={summary.cycle_completed} latest={latest_candle or 'n/a'}"
    )


if __name__ == "__main__":
    main()

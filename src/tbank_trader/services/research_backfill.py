from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Protocol

from sqlalchemy.orm import Session

from tbank_trader.config import AppSettings, get_settings
from tbank_trader.services.historical_data import normalize_candle_payload
from tbank_trader.services.instrument_catalog import sync_instrument_catalog
from tbank_trader.services.tbank_client import TBankRestClient, parse_api_timestamp, quotation_to_float
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    count_dividend_events,
    count_historical_candles,
    get_eligible_russian_shares,
    load_dividend_events_for_instruments,
    load_historical_candles_for_instruments,
    load_instrument_research_status_for_instruments,
    upsert_dividend_events,
    upsert_historical_candles,
    upsert_instrument_research_status,
)


@dataclass(slots=True)
class ResearchBackfillSummary:
    russian_shares: int
    candles_written: int
    dividend_events_written: int
    status_snapshots_written: int
    total_daily_candles: int
    total_dividend_events: int
    history_ready_instruments: int
    export_dir: str | None


class ResearchSource(Protocol):
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

    def get_dividends(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
    ) -> list[dict]:
        raise NotImplementedError

    def get_instrument_by_uid(self, instrument_uid: str) -> dict:
        raise NotImplementedError


def _iter_time_windows(
    *,
    start_at: datetime,
    end_at: datetime,
    step_days: int,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start_at
    while cursor < end_at:
        window_end = min(cursor + timedelta(days=step_days), end_at)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def _build_dividend_event_id(
    *,
    instrument_uid: str,
    record_date: datetime | None,
    payment_date: datetime | None,
    declared_date: datetime | None,
    dividend_type: str,
    dividend_net: float,
    currency: str,
) -> str:
    payload = "|".join(
        [
            instrument_uid,
            record_date.isoformat() if record_date is not None else "",
            payment_date.isoformat() if payment_date is not None else "",
            declared_date.isoformat() if declared_date is not None else "",
            dividend_type,
            f"{dividend_net:.12f}",
            currency,
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def normalize_dividend_payload(
    *,
    instrument_uid: str,
    dividend: dict,
) -> dict[str, object]:
    dividend_net_value = dividend.get("dividendNet")
    close_price_value = dividend.get("closePrice")
    record_date = parse_api_timestamp(dividend.get("recordDate"))
    payment_date = parse_api_timestamp(dividend.get("paymentDate"))
    declared_date = parse_api_timestamp(dividend.get("declaredDate"))
    last_buy_date = parse_api_timestamp(dividend.get("lastBuyDate"))
    created_at_event = parse_api_timestamp(dividend.get("createdAt"))
    dividend_type = str(dividend.get("dividendType", "") or "")
    currency = str((dividend_net_value or {}).get("currency", "") or "")
    dividend_net = quotation_to_float(dividend_net_value)
    return {
        "event_id": _build_dividend_event_id(
            instrument_uid=instrument_uid,
            record_date=record_date,
            payment_date=payment_date,
            declared_date=declared_date,
            dividend_type=dividend_type,
            dividend_net=dividend_net,
            currency=currency,
        ),
        "instrument_uid": instrument_uid,
        "record_date": record_date,
        "payment_date": payment_date,
        "declared_date": declared_date,
        "last_buy_date": last_buy_date,
        "created_at_event": created_at_event,
        "dividend_type": dividend_type,
        "regularity": str(dividend.get("regularity", "") or ""),
        "currency": currency,
        "dividend_net": dividend_net,
        "close_price": quotation_to_float(close_price_value),
        "yield_value": quotation_to_float(dividend.get("yieldValue")),
    }


def normalize_instrument_status_payload(
    *,
    instrument_uid: str,
    instrument: dict,
) -> dict[str, object]:
    return {
        "instrument_uid": instrument_uid,
        "trading_status": str(instrument.get("tradingStatus", "") or ""),
        "buy_available_flag": bool(instrument.get("buyAvailableFlag", False)),
        "sell_available_flag": bool(instrument.get("sellAvailableFlag", False)),
        "api_trade_available_flag": bool(instrument.get("apiTradeAvailableFlag", False)),
        "otc_flag": bool(instrument.get("otcFlag", False)),
        "blocked_tca_flag": bool(instrument.get("blockedTcaFlag", False)),
        "first_1min_candle_date": parse_api_timestamp(instrument.get("first1MinCandleDate")),
        "first_1day_candle_date": parse_api_timestamp(instrument.get("first1DayCandleDate")),
    }


def backfill_ru_share_research_data(
    session: Session,
    *,
    settings: AppSettings,
    source: ResearchSource,
    start_at: datetime,
    end_at: datetime,
    interval: str = "CANDLE_INTERVAL_DAY",
    instrument_limit: int | None = None,
    chunk_days: int = 300,
    request_pause_seconds: float = 0.3,
    export_dir: Path | None = None,
) -> ResearchBackfillSummary:
    instruments = get_eligible_russian_shares(session)
    if instrument_limit is not None:
        instruments = instruments[:instrument_limit]

    candles_written = 0
    dividend_events_written = 0
    status_snapshots_written = 0

    candle_windows = _iter_time_windows(start_at=start_at, end_at=end_at, step_days=chunk_days)
    for instrument in instruments:
        instrument_uid = str(instrument["instrument_uid"])

        candle_rows_by_time: dict[datetime, dict[str, object]] = {}
        for from_, to in candle_windows:
            raw_candles = source.get_candles(
                instrument_id=instrument_uid,
                from_=from_,
                to=to,
                interval=interval,
                limit=settings.historical_backfill_limit_per_request,
            )
            for candle in raw_candles:
                normalized = normalize_candle_payload(
                    instrument_uid=instrument_uid,
                    interval=interval,
                    candle=candle,
                )
                if normalized is None:
                    continue
                candle_rows_by_time[normalized["candle_time"]] = normalized
            if request_pause_seconds > 0:
                time.sleep(request_pause_seconds)

        if candle_rows_by_time:
            normalized_candles = [candle_rows_by_time[key] for key in sorted(candle_rows_by_time)]
            upsert_historical_candles(session, normalized_candles)
            candles_written += len(normalized_candles)

        dividends = source.get_dividends(
            instrument_id=instrument_uid,
            from_=start_at,
            to=end_at,
        )
        if dividends:
            normalized_dividends = [
                normalize_dividend_payload(
                    instrument_uid=instrument_uid,
                    dividend=dividend,
                )
                for dividend in dividends
            ]
            upsert_dividend_events(session, normalized_dividends)
            dividend_events_written += len(normalized_dividends)
        if request_pause_seconds > 0:
            time.sleep(request_pause_seconds)

        instrument_status = source.get_instrument_by_uid(instrument_uid)
        if instrument_status:
            upsert_instrument_research_status(
                session,
                [normalize_instrument_status_payload(instrument_uid=instrument_uid, instrument=instrument_status)],
            )
            status_snapshots_written += 1
        session.commit()
        if request_pause_seconds > 0:
            time.sleep(request_pause_seconds)

    if export_dir is not None:
        export_research_dataset(
            session,
            instruments=instruments,
            interval=interval,
            export_dir=export_dir,
        )

    history_ready_instruments = 0
    if instruments:
        instrument_uids = [str(instrument["instrument_uid"]) for instrument in instruments]
        candle_rows = load_historical_candles_for_instruments(
            session,
            instrument_uids=instrument_uids,
            interval=interval,
        )
        grouped: dict[str, list[dict[str, object]]] = {}
        for row in candle_rows:
            grouped.setdefault(str(row["instrument_uid"]), []).append(row)
        for instrument in instruments:
            rows = grouped.get(str(instrument["instrument_uid"]), [])
            completed_rows = [row for row in rows if bool(row["is_complete"])]
            turnovers = [float(row["turnover_rub"]) for row in completed_rows]
            if (
                len(completed_rows) >= settings.historical_min_candle_count
                and (float(median(turnovers)) if turnovers else 0.0) >= settings.universe_min_median_turnover_rub
            ):
                history_ready_instruments += 1

    return ResearchBackfillSummary(
        russian_shares=len(instruments),
        candles_written=candles_written,
        dividend_events_written=dividend_events_written,
        status_snapshots_written=status_snapshots_written,
        total_daily_candles=count_historical_candles(session, interval=interval),
        total_dividend_events=count_dividend_events(session),
        history_ready_instruments=history_ready_instruments,
        export_dir=str(export_dir) if export_dir is not None else None,
    )


def export_research_dataset(
    session: Session,
    *,
    instruments: list[dict[str, object]],
    interval: str,
    export_dir: Path,
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    instrument_uids = [str(instrument["instrument_uid"]) for instrument in instruments]

    with (export_dir / "instruments.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "instrument_uid",
                "figi",
                "ticker",
                "class_code",
                "instrument_type",
                "name",
                "lot",
                "currency",
                "exchange",
                "country_of_risk",
            ],
        )
        writer.writeheader()
        writer.writerows(instruments)

    candle_rows = load_historical_candles_for_instruments(
        session,
        instrument_uids=instrument_uids,
        interval=interval,
    )
    with (export_dir / "daily_candles.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "instrument_uid",
                "interval",
                "candle_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "turnover_rub",
                "is_complete",
            ],
        )
        writer.writeheader()
        for row in candle_rows:
            serialized = dict(row)
            serialized["candle_time"] = serialized["candle_time"].isoformat()
            writer.writerow(serialized)

    dividend_rows = load_dividend_events_for_instruments(session, instrument_uids=instrument_uids)
    with (export_dir / "dividends.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "event_id",
                "instrument_uid",
                "record_date",
                "payment_date",
                "declared_date",
                "last_buy_date",
                "created_at_event",
                "dividend_type",
                "regularity",
                "currency",
                "dividend_net",
                "close_price",
                "yield_value",
            ],
        )
        writer.writeheader()
        for row in dividend_rows:
            serialized = dict(row)
            for key in (
                "record_date",
                "payment_date",
                "declared_date",
                "last_buy_date",
                "created_at_event",
            ):
                value = serialized.get(key)
                serialized[key] = value.isoformat() if value is not None else ""
            writer.writerow(serialized)

    status_rows = load_instrument_research_status_for_instruments(session, instrument_uids=instrument_uids)
    with (export_dir / "instrument_status.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "instrument_uid",
                "trading_status",
                "buy_available_flag",
                "sell_available_flag",
                "api_trade_available_flag",
                "otc_flag",
                "blocked_tca_flag",
                "first_1min_candle_date",
                "first_1day_candle_date",
                "snapshot_at",
            ],
        )
        writer.writeheader()
        for row in status_rows:
            serialized = dict(row)
            for key in ("first_1min_candle_date", "first_1day_candle_date", "snapshot_at"):
                value = serialized.get(key)
                serialized[key] = value.isoformat() if value is not None else ""
            writer.writerow(serialized)

    summary = {
        "instrument_count": len(instruments),
        "daily_candle_count": len(candle_rows),
        "dividend_event_count": len(dividend_rows),
        "status_snapshot_count": len(status_rows),
        "interval": interval,
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }
    (export_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Russian-share research data from T-Invest API.")
    parser.add_argument("--start", default="2022-01-01", help="UTC start date in YYYY-MM-DD format.")
    parser.add_argument("--end", default=None, help="UTC end date in YYYY-MM-DD format. Default: now.")
    parser.add_argument("--interval", default="CANDLE_INTERVAL_DAY", help="T-Invest candle interval enum.")
    parser.add_argument("--instrument-limit", type=int, default=None, help="Optional cap on number of instruments.")
    parser.add_argument("--chunk-days", type=int, default=300, help="Chunk size in days for candle requests.")
    parser.add_argument("--request-pause", type=float, default=0.3, help="Pause between broker requests.")
    parser.add_argument(
        "--export-dir",
        default="data_exports/ru_shares_2022_daily",
        help="Directory for CSV/JSON export. Use empty value to disable exports.",
    )
    args = parser.parse_args()

    settings = get_settings()
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)
    start_at = datetime.fromisoformat(f"{args.start}T00:00:00+00:00")
    end_at = (
        datetime.fromisoformat(f"{args.end}T00:00:00+00:00")
        if args.end
        else datetime.now(timezone.utc)
    )
    export_dir = Path(args.export_dir) if args.export_dir else None

    client = TBankRestClient(settings)
    try:
        with session_factory() as session:
            bootstrap_defaults(session, settings)
            sync_instrument_catalog(session, settings=settings, source=client)
            summary = backfill_ru_share_research_data(
                session,
                settings=settings,
                source=client,
                start_at=start_at,
                end_at=end_at,
                interval=args.interval,
                instrument_limit=args.instrument_limit,
                chunk_days=args.chunk_days,
                request_pause_seconds=args.request_pause,
                export_dir=export_dir,
            )
    finally:
        client.close()

    print(
        f"Research backfill completed: russian_shares={summary.russian_shares} "
        f"candles_written={summary.candles_written} dividend_events_written={summary.dividend_events_written} "
        f"status_snapshots_written={summary.status_snapshots_written} total_daily_candles={summary.total_daily_candles} "
        f"total_dividend_events={summary.total_dividend_events} history_ready={summary.history_ready_instruments} "
        f"export_dir={summary.export_dir or 'disabled'}"
    )


if __name__ == "__main__":
    main()

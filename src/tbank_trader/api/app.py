from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from tbank_trader.config import AppSettings, get_settings
from tbank_trader.services.event_bus import EventBus
from tbank_trader.services.tbank_client import TBankApiError, TBankRestClient
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    build_dashboard,
    get_run_offsets,
    get_state,
    count_historical_candles,
    count_history_ready_instruments,
    count_instruments_with_historical_candles,
    get_latest_historical_candle_at,
    serialize_instrument_universe,
    serialize_historical_simulation_rebalances,
    serialize_latest_historical_simulation,
    serialize_orders,
    serialize_positions,
    serialize_signals,
    set_state,
)


def create_app(settings: AppSettings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    package_dir = Path(__file__).resolve().parent.parent
    templates = Jinja2Templates(directory=str(package_dir / "templates"))

    app = FastAPI(title=resolved_settings.app_name)
    app.mount("/static", StaticFiles(directory=str(package_dir / "static")), name="static")

    engine = build_engine(resolved_settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(resolved_settings.database_url)
    event_bus = EventBus(resolved_settings.redis_url)

    with session_factory() as session:
        bootstrap_defaults(session, resolved_settings)

    app.state.settings = resolved_settings
    app.state.session_factory = session_factory
    app.state.event_bus = event_bus
    app.state.templates = templates
    app.state.broker_status_cache = {
        "expires_at": datetime.now(timezone.utc),
        "payload": None,
    }

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "app_name": resolved_settings.app_name,
                "refresh_seconds": resolved_settings.dashboard_refresh_seconds,
                "broker_status_refresh_seconds": resolved_settings.broker_status_cache_seconds,
            },
        )

    @app.get("/health")
    def health() -> dict[str, object]:
        db_ok = False
        with session_factory() as session:
            build_dashboard(session, settings=resolved_settings)
            db_ok = True

        return {
            "status": "ok",
            "broker_mode": resolved_settings.broker_mode,
            "database": db_ok,
            "redis": event_bus.ping() if resolved_settings.redis_url else False,
        }

    @app.get("/api/dashboard")
    def dashboard() -> dict[str, object]:
        with session_factory() as session:
            return build_dashboard(session, settings=resolved_settings)

    @app.get("/api/broker/status")
    def broker_status() -> dict[str, object]:
        if resolved_settings.broker_mode != "tbank":
            return {
                "configured": False,
                "mode": resolved_settings.broker_mode,
                "message": "Broker is running in simulated mode",
            }

        cached = app.state.broker_status_cache
        now = datetime.now(timezone.utc)
        if (
            cached.get("payload") is not None
            and isinstance(cached.get("expires_at"), datetime)
            and now < cached["expires_at"]
        ):
            return cached["payload"]

        with session_factory() as session:
            account_id = resolved_settings.tbank_account_id or get_state(session, "broker.account_id")

        client = TBankRestClient(resolved_settings)
        try:
            payload = client.build_status(resolved_settings.symbols, preferred_account_id=account_id)
        except TBankApiError as exc:
            return {
                "configured": False,
                "mode": resolved_settings.broker_mode,
                "sandbox": resolved_settings.tbank_use_sandbox,
                "error": str(exc),
            }
        finally:
            client.close()

        payload["mode"] = resolved_settings.broker_mode
        app.state.broker_status_cache = {
            "expires_at": now + timedelta(seconds=resolved_settings.broker_status_cache_seconds),
            "payload": payload,
        }
        return payload

    @app.get("/api/orders")
    def orders(limit: int = 25, scope: Literal["current", "all"] = "current") -> list[dict[str, object]]:
        with session_factory() as session:
            order_offset, _ = get_run_offsets(session)
            min_id = order_offset if scope == "current" else 0
            return serialize_orders(session, limit=limit, min_id=min_id)

    @app.get("/api/signals")
    def signals(limit: int = 25, scope: Literal["current", "all"] = "current") -> list[dict[str, object]]:
        with session_factory() as session:
            _, signal_offset = get_run_offsets(session)
            min_id = signal_offset if scope == "current" else 0
            return serialize_signals(session, limit=limit, min_id=min_id)

    @app.get("/api/positions")
    def positions() -> list[dict[str, object]]:
        with session_factory() as session:
            return serialize_positions(session)

    @app.get("/api/universe/catalog")
    def universe_catalog(
        limit: int = 200,
        eligible_only: bool = False,
    ) -> list[dict[str, object]]:
        with session_factory() as session:
            return serialize_instrument_universe(
                session,
                limit=limit,
                eligible_only=eligible_only,
            )

    @app.get("/api/universe/history")
    def universe_history() -> dict[str, object]:
        with session_factory() as session:
            return {
                "interval": resolved_settings.strategy_candle_interval,
                "candles": count_historical_candles(
                    session,
                    interval=resolved_settings.strategy_candle_interval,
                ),
                "covered_instruments": count_instruments_with_historical_candles(
                    session,
                    interval=resolved_settings.strategy_candle_interval,
                ),
                "history_ready_instruments": count_history_ready_instruments(
                    session,
                    interval=resolved_settings.strategy_candle_interval,
                ),
                "latest_candle_at": get_latest_historical_candle_at(
                    session,
                    interval=resolved_settings.strategy_candle_interval,
                ),
                "historical_regime_mode": resolved_settings.historical_regime_mode,
                "historical_regime_spx_ticker": resolved_settings.historical_regime_spx_ticker,
                "historical_regime_vix_ticker": resolved_settings.historical_regime_vix_ticker,
                "historical_regime_vix_threshold": resolved_settings.historical_regime_vix_threshold,
                "historical_ranking_price_role": "daily_close_prev_day_raw_placeholder_until_dividend_adjustment",
                "historical_execution_price_role": "daily_open_from_intraday",
                "min_candle_count": resolved_settings.historical_min_candle_count,
                "min_median_turnover_rub": resolved_settings.universe_min_median_turnover_rub,
                "backfill_cursor": int(get_state(session, "history.backfill_cursor", "0") or "0"),
                "backfill_last_batch_at": get_state(session, "history.backfill_last_batch_at", "n/a"),
                "backfill_last_batch_size": int(get_state(session, "history.backfill_last_batch_size", "0") or "0"),
                "backfill_last_batch_written": int(get_state(session, "history.backfill_last_batch_written", "0") or "0"),
                "backfill_cycle_completed": get_state(session, "history.backfill_cycle_completed", "false") == "true",
            }

    @app.get("/api/simulation/latest")
    def latest_simulation() -> dict[str, object]:
        with session_factory() as session:
            run = serialize_latest_historical_simulation(session)
            if run is None:
                return {"run": None, "rebalances": []}
            return {
                "run": run,
                "rebalances": serialize_historical_simulation_rebalances(
                    session,
                    run_id=int(run["id"]),
                    limit=50,
                ),
            }

    @app.get("/api/system")
    def system() -> dict[str, object]:
        with session_factory() as session:
            dashboard_payload = build_dashboard(session, settings=resolved_settings)
            return dashboard_payload["system"]

    @app.post("/api/system/pause")
    def pause() -> dict[str, str]:
        with session_factory() as session:
            set_state(session, "system.paused", "true")
            session.commit()
        return {"status": "paused"}

    @app.post("/api/system/resume")
    def resume() -> dict[str, str]:
        with session_factory() as session:
            set_state(session, "system.paused", "false")
            session.commit()
        return {"status": "running"}

    return app

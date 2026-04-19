from __future__ import annotations

from datetime import datetime, timedelta, timezone
import time

from tbank_trader.broker.base import BrokerAdapter, BrokerInstrument, BrokerOrderResult, BrokerPositionSnapshot
from tbank_trader.config import AppSettings
from tbank_trader.services.tbank_client import TBankRestClient, quotation_to_float


class TBankBrokerAdapter(BrokerAdapter):
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.client = TBankRestClient(settings)
        self.account_id = self.client.ensure_sandbox_account(settings.tbank_account_id)
        self.client.ensure_min_rub_balance(self.account_id)
        self._last_order_monotonic = 0.0
        self.instruments = {
            symbol: self.client.resolve_symbol(symbol)
            for symbol in settings.symbols
        }
        self._broker_instruments = {
            symbol: BrokerInstrument(
                symbol=symbol,
                lot=instrument.lot,
                instrument_type=instrument.instrument_type,
                class_code=instrument.class_code,
                instrument_uid=instrument.instrument_uid,
                figi=instrument.figi,
            )
            for symbol, instrument in self.instruments.items()
        }
        self.symbol_by_instrument_uid = {
            instrument.instrument_uid: symbol
            for symbol, instrument in self.instruments.items()
        }

    def _get_or_resolve_instrument(self, symbol: str):
        instrument = self.instruments.get(symbol)
        if instrument is not None:
            return instrument

        resolved = self.client.resolve_symbol(symbol)
        self.instruments[symbol] = resolved
        self._broker_instruments[symbol] = BrokerInstrument(
            symbol=symbol,
            lot=resolved.lot,
            instrument_type=resolved.instrument_type,
            class_code=resolved.class_code,
            instrument_uid=resolved.instrument_uid,
            figi=resolved.figi,
        )
        self.symbol_by_instrument_uid[resolved.instrument_uid] = symbol
        return resolved

    def get_instruments(self) -> dict[str, BrokerInstrument]:
        return self._broker_instruments

    def next_price(self, symbol: str) -> float:
        prices = self.get_prices([symbol])
        price = prices.get(symbol)
        if price is None:
            raise RuntimeError(f"Missing last price for symbol={symbol}")
        return price

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        instrument_uids = [self._get_or_resolve_instrument(symbol).instrument_uid for symbol in symbols]
        prices_by_uid = self.client.get_last_prices(instrument_uids)
        return {
            symbol: prices_by_uid[instrument_uid]
            for symbol, instrument_uid in zip(symbols, instrument_uids)
            if instrument_uid in prices_by_uid
        }

    def place_order(self, *, symbol: str, side: str, quantity: int, price: float) -> BrokerOrderResult:
        min_interval = max(self.settings.tbank_min_order_interval_seconds, 0.0)
        if min_interval > 0:
            elapsed = time.monotonic() - self._last_order_monotonic
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        instrument = self._get_or_resolve_instrument(symbol)
        response = self.client.post_sandbox_market_order(
            account_id=self.account_id,
            instrument_id=instrument.instrument_uid,
            side=side,
            quantity=quantity,
        )
        self._last_order_monotonic = time.monotonic()
        return BrokerOrderResult(
            broker_order_id=response["orderId"],
            status=response.get("executionReportStatus", "unknown"),
        )

    def get_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        portfolio = self.client.get_sandbox_portfolio(self.account_id)
        snapshots: list[BrokerPositionSnapshot] = []
        for row in portfolio.get("positions", []):
            instrument_uid = row.get("instrumentUid")
            symbol = self.symbol_by_instrument_uid.get(instrument_uid) or row.get("ticker")
            if symbol not in self.instruments:
                continue
            snapshots.append(
                BrokerPositionSnapshot(
                    symbol=symbol,
                    quantity=int(row.get("quantityLots", {}).get("units", "0")),
                    avg_price=quotation_to_float(row.get("averagePositionPrice")),
                    market_price=quotation_to_float(row.get("currentPrice")),
                )
            )
        return snapshots

    def get_cash_balance_rub(self) -> float:
        positions = self.client.get_sandbox_positions(self.account_id)
        for money in positions.get("money", []):
            if str(money.get("currency", "")).upper() == "RUB":
                return quotation_to_float(money)
        return 0.0

    def get_historical_closes(self, *, symbol: str, limit: int, interval: str) -> list[float]:
        instrument = self._get_or_resolve_instrument(symbol)
        now = datetime.now(timezone.utc)
        lookback_hours = max(limit * 8, 72)
        from_ = now - timedelta(hours=lookback_hours)
        candles = self.client.get_candles(
            instrument_id=instrument.instrument_uid,
            from_=from_,
            to=now,
            interval=interval,
            limit=limit,
        )
        return [
            quotation_to_float(candle.get("close"))
            for candle in candles
            if candle.get("isComplete")
        ][-limit:]

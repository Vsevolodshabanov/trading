from __future__ import annotations

import random
import uuid

from tbank_trader.broker.base import BrokerAdapter, BrokerInstrument, BrokerOrderResult, BrokerPositionSnapshot


class SimulatedBrokerAdapter(BrokerAdapter):
    def __init__(self, symbols: list[str], seed: int, starting_cash_rub: float = 1_000_000.0) -> None:
        self._rng = random.Random(seed)
        self._prices = {
            symbol: 100.0 + index * 17.0 for index, symbol in enumerate(symbols)
        }
        self._instruments = {
            symbol: BrokerInstrument(
                symbol=symbol,
                lot=1,
                instrument_type="share",
                class_code="SIM",
            )
            for symbol in symbols
        }
        self._positions = {
            symbol: 0 for symbol in symbols
        }
        self._cash_rub = starting_cash_rub
        self._history = {
            symbol: self._bootstrap_history(price)
            for symbol, price in self._prices.items()
        }

    def _bootstrap_history(self, base_price: float) -> list[float]:
        history = [round(base_price, 4)]
        for _ in range(239):
            drift = self._rng.uniform(-1.2, 1.4)
            next_price = max(1.0, history[-1] + drift)
            history.append(round(next_price, 4))
        return history

    def _ensure_symbol(self, symbol: str) -> None:
        if symbol in self._prices:
            return
        base_price = 100.0 + len(self._prices) * 17.0
        self._prices[symbol] = base_price
        self._instruments[symbol] = BrokerInstrument(
            symbol=symbol,
            lot=1,
            instrument_type="share",
            class_code="SIM",
        )
        self._positions[symbol] = 0
        self._history[symbol] = self._bootstrap_history(base_price)

    def get_instruments(self) -> dict[str, BrokerInstrument]:
        return self._instruments

    def next_price(self, symbol: str) -> float:
        self._ensure_symbol(symbol)
        base_price = self._prices[symbol]
        drift = self._rng.uniform(-0.8, 0.8)
        next_price = max(1.0, base_price + drift)
        self._prices[symbol] = round(next_price, 4)
        self._history[symbol].append(self._prices[symbol])
        self._history[symbol] = self._history[symbol][-500:]
        return self._prices[symbol]

    def place_order(self, *, symbol: str, side: str, quantity: int, price: float) -> BrokerOrderResult:
        self._ensure_symbol(symbol)
        notional = quantity * price * self._instruments[symbol].lot
        if side == "buy":
            self._positions[symbol] += quantity
            self._cash_rub -= notional
        else:
            self._positions[symbol] -= quantity
            self._cash_rub += notional
        return BrokerOrderResult(
            broker_order_id=f"sim-{uuid.uuid4()}",
            status="filled",
        )

    def get_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        return [
            BrokerPositionSnapshot(
                symbol=symbol,
                quantity=quantity,
                avg_price=self._prices[symbol],
                market_price=self._prices[symbol],
            )
            for symbol, quantity in self._positions.items()
            if quantity != 0
        ]

    def get_cash_balance_rub(self) -> float:
        return self._cash_rub

    def get_historical_closes(self, *, symbol: str, limit: int, interval: str) -> list[float]:
        _ = interval
        self._ensure_symbol(symbol)
        history = self._history[symbol]
        if len(history) < limit:
            for _ in range(limit - len(history)):
                self.next_price(symbol)
        return history[-limit:]

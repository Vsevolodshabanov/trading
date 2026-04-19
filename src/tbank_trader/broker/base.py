from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class BrokerOrderResult:
    broker_order_id: str
    status: str


@dataclass(slots=True)
class BrokerInstrument:
    symbol: str
    lot: int
    instrument_type: str
    class_code: str
    instrument_uid: str | None = None
    figi: str | None = None


@dataclass(slots=True)
class BrokerPositionSnapshot:
    symbol: str
    quantity: int
    avg_price: float
    market_price: float


class BrokerAdapter:
    def next_price(self, symbol: str) -> float:
        raise NotImplementedError

    def get_instruments(self) -> dict[str, BrokerInstrument]:
        raise NotImplementedError

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        return {symbol: self.next_price(symbol) for symbol in symbols}

    def place_order(self, *, symbol: str, side: str, quantity: int, price: float) -> BrokerOrderResult:
        raise NotImplementedError

    def get_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        return []

    def get_cash_balance_rub(self) -> float:
        return 0.0

    def get_historical_closes(self, *, symbol: str, limit: int, interval: str) -> list[float]:
        raise NotImplementedError

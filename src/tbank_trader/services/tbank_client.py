from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx

from tbank_trader.config import AppSettings


class TBankApiError(RuntimeError):
    pass


@dataclass(slots=True)
class InstrumentRef:
    symbol: str
    instrument_uid: str
    figi: str
    ticker: str
    class_code: str
    instrument_type: str
    lot: int
    name: str


@dataclass(slots=True)
class CatalogInstrument:
    instrument_uid: str
    figi: str
    ticker: str
    class_code: str
    instrument_type: str
    name: str
    lot: int
    currency: str
    exchange: str
    country_of_risk: str
    buy_available_flag: bool
    sell_available_flag: bool
    api_trade_available_flag: bool
    for_iis_flag: bool
    for_qual_investor_flag: bool
    weekend_flag: bool
    otc_flag: bool
    active_flag: bool


def quotation_to_float(value: dict[str, Any] | None) -> float:
    if not value:
        return 0.0

    units = Decimal(str(value.get("units", "0")))
    nano = Decimal(str(value.get("nano", 0))) / Decimal("1000000000")
    return float(units + nano)


def parse_api_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def select_best_instrument(symbol: str, instruments: list[InstrumentRef]) -> InstrumentRef:
    if not instruments:
        raise TBankApiError(f"Instrument not found for symbol={symbol}")

    symbol_upper = symbol.upper()

    def score(instrument: InstrumentRef) -> tuple[int, int]:
        value = 0
        if instrument.instrument_uid.upper() == symbol_upper:
            value += 1_000
        if instrument.figi.upper() == symbol_upper:
            value += 900
        if instrument.ticker.upper() == symbol_upper:
            value += 800
        if f"{instrument.ticker}_{instrument.class_code}".upper() == symbol_upper:
            value += 700
        if instrument.class_code == "TQBR":
            value += 50
        if instrument.class_code == "CETS":
            value += 45
        if instrument.class_code == "TQCB":
            value += 40
        if instrument.instrument_type == "share":
            value += 10
        if instrument.instrument_type == "currency":
            value += 9
        if instrument.instrument_type == "bond":
            value += 8
        return value, -len(instrument.name)

    return max(instruments, key=score)


class TBankRestClient:
    def __init__(self, settings: AppSettings) -> None:
        if not settings.tbank_token:
            raise ValueError("T-Bank token is required for tbank mode")

        if not settings.tbank_use_sandbox:
            raise ValueError("Only sandbox mode is implemented for T-Bank in this iteration")

        self.settings = settings
        self.client = httpx.Client(
            base_url="https://sandbox-invest-public-api.tbank.ru/rest",
            headers={
                "Authorization": f"Bearer {settings.tbank_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-app-name": settings.app_name,
            },
            timeout=settings.tbank_timeout_seconds,
            verify=settings.tbank_verify_ssl,
        )

    def close(self) -> None:
        self.client.close()

    def _rpc(self, service: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = f"/tinkoff.public.invest.api.contract.v1.{service}/{method}"
        try:
            response = self.client.post(endpoint, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = exc.response.text
            raise TBankApiError(f"T-Bank HTTP error {exc.response.status_code}: {body}") from exc
        except httpx.HTTPError as exc:
            raise TBankApiError(f"T-Bank transport error: {exc}") from exc

        data = response.json()
        if isinstance(data, dict) and data.get("code") and data.get("message"):
            raise TBankApiError(f"T-Bank API error {data['code']}: {data['message']}")
        return data

    def get_sandbox_accounts(self) -> list[dict[str, Any]]:
        data = self._rpc("SandboxService", "GetSandboxAccounts", {})
        return data.get("accounts", [])

    def open_sandbox_account(self) -> str:
        data = self._rpc("SandboxService", "OpenSandboxAccount", {})
        account_id = data.get("accountId")
        if not account_id:
            raise TBankApiError("Sandbox account was created without accountId")
        return account_id

    def ensure_sandbox_account(self, preferred_account_id: str | None = None) -> str:
        accounts = self.get_sandbox_accounts()
        account_ids = [
            account.get("id") or account.get("accountId")
            for account in accounts
            if account.get("id") or account.get("accountId")
        ]

        if preferred_account_id and preferred_account_id in account_ids:
            return preferred_account_id

        if account_ids:
            return account_ids[0]

        if not self.settings.tbank_sandbox_auto_create_account:
            raise TBankApiError("Sandbox account is missing and auto-create is disabled")

        return self.open_sandbox_account()

    def sandbox_pay_in(self, account_id: str, units: int, currency: str = "RUB") -> float:
        data = self._rpc(
            "SandboxService",
            "SandboxPayIn",
            {
                "accountId": account_id,
                "amount": {
                    "currency": currency,
                    "units": str(units),
                    "nano": 0,
                },
            },
        )
        return quotation_to_float(data.get("balance"))

    def get_sandbox_positions(self, account_id: str) -> dict[str, Any]:
        return self._rpc("SandboxService", "GetSandboxPositions", {"accountId": account_id})

    def get_sandbox_portfolio(self, account_id: str, currency: str = "RUB") -> dict[str, Any]:
        return self._rpc(
            "SandboxService",
            "GetSandboxPortfolio",
            {
                "accountId": account_id,
                "currency": currency,
            },
        )

    def ensure_min_rub_balance(self, account_id: str) -> float:
        required = self.settings.tbank_sandbox_min_rub_balance
        if required <= 0:
            return 0.0

        positions = self.get_sandbox_positions(account_id)
        current_rub_balance = 0.0
        for money in positions.get("money", []):
            if str(money.get("currency", "")).upper() == "RUB":
                current_rub_balance = quotation_to_float(money)
                break

        if current_rub_balance >= required:
            return current_rub_balance

        return self.sandbox_pay_in(account_id, int(required - current_rub_balance), currency="RUB")

    def find_instrument(self, query: str) -> list[InstrumentRef]:
        data = self._rpc(
            "InstrumentsService",
            "FindInstrument",
            {
                "query": query,
                "instrumentKind": "INSTRUMENT_TYPE_UNSPECIFIED",
                "apiTradeAvailableFlag": True,
            },
        )
        results: list[InstrumentRef] = []
        for row in data.get("instruments", []):
            instrument_uid = row.get("uid")
            figi = row.get("figi")
            ticker = row.get("ticker")
            class_code = row.get("classCode")
            if not instrument_uid or not figi or not ticker or not class_code:
                continue
            results.append(
                InstrumentRef(
                    symbol=query,
                    instrument_uid=instrument_uid,
                    figi=figi,
                    ticker=ticker,
                    class_code=class_code,
                    instrument_type=row.get("instrumentType", ""),
                    lot=int(row.get("lot", 1)),
                    name=row.get("name", ""),
                )
            )
        return results

    def _extract_catalog_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("instruments", "shares", "bonds", "currencies", "etfs", "futures"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []

    def _normalize_catalog_instrument(
        self,
        row: dict[str, Any],
        *,
        fallback_instrument_type: str,
    ) -> CatalogInstrument | None:
        instrument_uid = row.get("uid")
        figi = row.get("figi")
        ticker = row.get("ticker")
        class_code = row.get("classCode")
        if not instrument_uid or not figi or not ticker or not class_code:
            return None

        return CatalogInstrument(
            instrument_uid=instrument_uid,
            figi=figi,
            ticker=ticker,
            class_code=class_code,
            instrument_type=row.get("instrumentType") or fallback_instrument_type,
            name=row.get("name", "") or ticker,
            lot=int(row.get("lot", 1) or 1),
            currency=row.get("currency", "") or "",
            exchange=row.get("exchange", "") or "",
            country_of_risk=row.get("countryOfRisk", "") or "",
            buy_available_flag=bool(row.get("buyAvailableFlag", False)),
            sell_available_flag=bool(row.get("sellAvailableFlag", False)),
            api_trade_available_flag=bool(row.get("apiTradeAvailableFlag", False)),
            for_iis_flag=bool(row.get("forIisFlag", False)),
            for_qual_investor_flag=bool(row.get("forQualInvestorFlag", False)),
            weekend_flag=bool(row.get("weekendFlag", False)),
            otc_flag=bool(row.get("otcFlag", False)),
            active_flag=not bool(row.get("blockedTcaFlag", False)),
        )

    def _list_catalog_instruments(self, method: str, *, instrument_type: str) -> list[CatalogInstrument]:
        data = self._rpc(
            "InstrumentsService",
            method,
            {
                "instrumentStatus": "INSTRUMENT_STATUS_ALL",
            },
        )
        instruments: list[CatalogInstrument] = []
        for row in self._extract_catalog_rows(data):
            normalized = self._normalize_catalog_instrument(
                row,
                fallback_instrument_type=instrument_type,
            )
            if normalized is not None:
                instruments.append(normalized)
        return instruments

    def list_shares(self) -> list[CatalogInstrument]:
        return self._list_catalog_instruments("Shares", instrument_type="share")

    def list_bonds(self) -> list[CatalogInstrument]:
        return self._list_catalog_instruments("Bonds", instrument_type="bond")

    def list_currencies(self) -> list[CatalogInstrument]:
        return self._list_catalog_instruments("Currencies", instrument_type="currency")

    def list_etfs(self) -> list[CatalogInstrument]:
        return self._list_catalog_instruments("Etfs", instrument_type="etf")

    def list_futures(self) -> list[CatalogInstrument]:
        return self._list_catalog_instruments("Futures", instrument_type="future")

    def list_all_catalog_instruments(self) -> list[CatalogInstrument]:
        by_uid: dict[str, CatalogInstrument] = {}
        for instrument in (
            self.list_shares()
            + self.list_bonds()
            + self.list_currencies()
            + self.list_etfs()
            + self.list_futures()
        ):
            by_uid[instrument.instrument_uid] = instrument
        return sorted(
            by_uid.values(),
            key=lambda item: (item.instrument_type, item.ticker, item.class_code),
        )

    def resolve_symbol(self, symbol: str) -> InstrumentRef:
        return select_best_instrument(symbol, self.find_instrument(symbol))

    def get_last_prices(self, instrument_ids: list[str]) -> dict[str, float]:
        data = self._rpc(
            "MarketDataService",
            "GetLastPrices",
            {"instrumentId": instrument_ids},
        )
        return {
            item["instrumentUid"]: quotation_to_float(item.get("price"))
            for item in data.get("lastPrices", [])
            if item.get("instrumentUid")
        }

    def get_candles(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
        interval: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        data = self._rpc(
            "MarketDataService",
            "GetCandles",
            {
                "instrumentId": instrument_id,
                "from": from_.isoformat().replace("+00:00", "Z"),
                "to": to.isoformat().replace("+00:00", "Z"),
                "interval": interval,
                "limit": limit,
            },
        )
        return data.get("candles", [])

    def get_dividends(
        self,
        *,
        instrument_id: str,
        from_: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        data = self._rpc(
            "InstrumentsService",
            "GetDividends",
            {
                "instrumentId": instrument_id,
                "from": from_.isoformat().replace("+00:00", "Z"),
                "to": to.isoformat().replace("+00:00", "Z"),
            },
        )
        return data.get("dividends", [])

    def get_instrument_by_uid(self, instrument_uid: str) -> dict[str, Any]:
        data = self._rpc(
            "InstrumentsService",
            "GetInstrumentBy",
            {
                "idType": "INSTRUMENT_ID_TYPE_UID",
                "id": instrument_uid,
            },
        )
        return data.get("instrument", {})

    def post_sandbox_market_order(
        self,
        *,
        account_id: str,
        instrument_id: str,
        side: str,
        quantity: int,
    ) -> dict[str, Any]:
        direction = "ORDER_DIRECTION_BUY" if side == "buy" else "ORDER_DIRECTION_SELL"
        return self._rpc(
            "SandboxService",
            "PostSandboxOrder",
            {
                "accountId": account_id,
                "instrumentId": instrument_id,
                "quantity": quantity,
                "direction": direction,
                "orderType": "ORDER_TYPE_MARKET",
                "orderId": str(uuid4()),
            },
        )

    def build_status(self, symbols: list[str], preferred_account_id: str | None = None) -> dict[str, Any]:
        account_id = self.ensure_sandbox_account(preferred_account_id)
        instruments = [self.resolve_symbol(symbol) for symbol in symbols]
        price_map = self.get_last_prices([item.instrument_uid for item in instruments])
        positions = self.get_sandbox_positions(account_id)
        portfolio = self.get_sandbox_portfolio(account_id)
        return {
            "configured": True,
            "sandbox": True,
            "account_id": account_id,
            "rub_balance": next(
                (
                    quotation_to_float(money)
                    for money in positions.get("money", [])
                    if str(money.get("currency", "")).upper() == "RUB"
                ),
                0.0,
            ),
            "total_portfolio": quotation_to_float(portfolio.get("totalAmountPortfolio")),
            "symbols": [
                {
                    "symbol": item.symbol,
                    "ticker": item.ticker,
                    "class_code": item.class_code,
                    "instrument_uid": item.instrument_uid,
                    "price": price_map.get(item.instrument_uid),
                    "lot": item.lot,
                }
                for item in instruments
            ],
            "security_positions": positions.get("securities", []),
        }

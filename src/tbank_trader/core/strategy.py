from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class SignalCandidate:
    side: str
    confidence: float
    reason: str


class BaseSignalGenerator:
    strategy_name = "base_signal"

    @property
    def required_history(self) -> int:
        raise NotImplementedError

    def warmup(self, prices: list[float]) -> None:
        raise NotImplementedError

    def on_price(self, price: float) -> SignalCandidate | None:
        raise NotImplementedError


class MovingAverageSignalGenerator(BaseSignalGenerator):
    strategy_name = "momentum_ma_cross"

    def __init__(
        self,
        *,
        short_window: int,
        long_window: int,
        threshold_bps: int,
    ) -> None:
        if short_window >= long_window:
            raise ValueError("short_window must be less than long_window")

        self.short_window = short_window
        self.long_window = long_window
        self.threshold_bps = threshold_bps
        self.history: deque[float] = deque(maxlen=long_window)
        self.last_direction: str | None = None

    @property
    def required_history(self) -> int:
        return self.long_window

    def warmup(self, prices: list[float]) -> None:
        for price in prices[-max(self.required_history - 1, 0) :]:
            self.history.append(price)

    def on_price(self, price: float) -> SignalCandidate | None:
        self.history.append(price)
        if len(self.history) < self.long_window:
            return None

        values = list(self.history)
        short_ma = sum(values[-self.short_window :]) / self.short_window
        long_ma = sum(values) / self.long_window
        spread_bps = (short_ma - long_ma) / long_ma * 10_000

        if spread_bps >= self.threshold_bps and self.last_direction != "buy":
            self.last_direction = "buy"
            return SignalCandidate(
                side="buy",
                confidence=min(abs(spread_bps) / self.threshold_bps, 5.0),
                reason=f"short_ma_above_long_ma:{spread_bps:.2f}bps",
            )

        if spread_bps <= -self.threshold_bps and self.last_direction != "sell":
            self.last_direction = "sell"
            return SignalCandidate(
                side="sell",
                confidence=min(abs(spread_bps) / self.threshold_bps, 5.0),
                reason=f"short_ma_below_long_ma:{spread_bps:.2f}bps",
            )

        return None


class MeanReversionSignalGenerator(BaseSignalGenerator):
    strategy_name = "bond_mean_reversion"

    def __init__(self, *, window: int, threshold_bps: int) -> None:
        if window < 2:
            raise ValueError("window must be at least 2")

        self.window = window
        self.threshold_bps = threshold_bps
        self.history: deque[float] = deque(maxlen=window)
        self.last_direction: str | None = None

    @property
    def required_history(self) -> int:
        return self.window

    def warmup(self, prices: list[float]) -> None:
        for price in prices[-max(self.required_history - 1, 0) :]:
            self.history.append(price)

    def on_price(self, price: float) -> SignalCandidate | None:
        self.history.append(price)
        if len(self.history) < self.window:
            return None

        mean_price = sum(self.history) / len(self.history)
        spread_bps = (price - mean_price) / mean_price * 10_000

        if spread_bps <= -self.threshold_bps and self.last_direction != "buy":
            self.last_direction = "buy"
            return SignalCandidate(
                side="buy",
                confidence=min(abs(spread_bps) / self.threshold_bps, 5.0),
                reason=f"price_below_mean:{spread_bps:.2f}bps",
            )

        if spread_bps >= self.threshold_bps and self.last_direction != "sell":
            self.last_direction = "sell"
            return SignalCandidate(
                side="sell",
                confidence=min(abs(spread_bps) / self.threshold_bps, 5.0),
                reason=f"price_above_mean:{spread_bps:.2f}bps",
            )

        return None


def build_strategy_generator(
    *,
    instrument_type: str,
    profile: str,
    short_window: int,
    long_window: int,
    threshold_bps: int,
) -> BaseSignalGenerator:
    profile_adjustments = {
        "active": {"short_shift": -1, "long_shift": -4, "threshold_mult": 0.6},
        "balanced": {"short_shift": 0, "long_shift": 0, "threshold_mult": 1.0},
        "conservative": {"short_shift": 1, "long_shift": 4, "threshold_mult": 1.5},
    }
    adjustment = profile_adjustments.get(profile, profile_adjustments["balanced"])
    tuned_short = max(3, short_window + adjustment["short_shift"])
    tuned_long = max(tuned_short + 2, long_window + adjustment["long_shift"])
    tuned_threshold = max(5, int(threshold_bps * adjustment["threshold_mult"]))

    if instrument_type == "bond":
        return MeanReversionSignalGenerator(
            window=tuned_long,
            threshold_bps=max(4, tuned_threshold // 2),
        )

    return MovingAverageSignalGenerator(
        short_window=tuned_short,
        long_window=tuned_long,
        threshold_bps=tuned_threshold,
    )

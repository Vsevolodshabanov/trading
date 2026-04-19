from tbank_trader.core.strategy import (
    MeanReversionSignalGenerator,
    MovingAverageSignalGenerator,
    build_strategy_generator,
)


def test_strategy_emits_buy_signal_on_uptrend() -> None:
    generator = MovingAverageSignalGenerator(
        short_window=3,
        long_window=5,
        threshold_bps=10,
    )
    prices = [100, 100.1, 100.2, 100.4, 100.7]
    signal = None
    for price in prices:
        signal = generator.on_price(price)

    assert signal is not None
    assert signal.side == "buy"


def test_strategy_emits_sell_signal_on_downtrend() -> None:
    generator = MovingAverageSignalGenerator(
        short_window=3,
        long_window=5,
        threshold_bps=10,
    )
    prices = [100.7, 100.4, 100.2, 100.0, 99.7]
    signal = None
    for price in prices:
        signal = generator.on_price(price)

    assert signal is not None
    assert signal.side == "sell"


def test_strategy_warmup_allows_first_live_tick_signal() -> None:
    generator = MovingAverageSignalGenerator(
        short_window=3,
        long_window=5,
        threshold_bps=10,
    )
    generator.warmup([100, 100.1, 100.2, 100.4])
    signal = generator.on_price(100.7)

    assert signal is not None
    assert signal.side == "buy"


def test_mean_reversion_strategy_buys_below_mean() -> None:
    generator = MeanReversionSignalGenerator(window=5, threshold_bps=10)
    generator.warmup([100.0, 100.0, 100.0, 100.0])
    signal = generator.on_price(99.7)

    assert signal is not None
    assert signal.side == "buy"


def test_strategy_factory_uses_bond_reversion() -> None:
    generator = build_strategy_generator(
        instrument_type="bond",
        profile="balanced",
        short_window=5,
        long_window=20,
        threshold_bps=20,
    )

    assert isinstance(generator, MeanReversionSignalGenerator)

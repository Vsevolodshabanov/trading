from tbank_trader.core.risk import RiskEngine


def test_risk_rejects_when_paused() -> None:
    engine = RiskEngine(max_position_per_symbol=3)
    decision = engine.evaluate(
        paused=True,
        symbol="SBER",
        side="buy",
        quantity=1,
        current_position=0,
    )
    assert decision.approved is False
    assert decision.reason == "system_paused"


def test_risk_rejects_position_limit() -> None:
    engine = RiskEngine(max_position_per_symbol=2)
    decision = engine.evaluate(
        paused=False,
        symbol="SBER",
        side="buy",
        quantity=1,
        current_position=2,
    )
    assert decision.approved is False
    assert "max_position_exceeded" in decision.reason


def test_risk_rejects_position_notional_limit() -> None:
    engine = RiskEngine(
        max_position_per_symbol=20,
        max_position_notional_rub=1_000,
    )
    decision = engine.evaluate(
        paused=False,
        symbol="SBER",
        side="buy",
        quantity=5,
        current_position=0,
        price=316.5,
        lot=1,
    )
    assert decision.approved is False
    assert "max_position_notional_exceeded" in decision.reason

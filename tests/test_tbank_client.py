from tbank_trader.services.tbank_client import InstrumentRef, quotation_to_float, select_best_instrument


def test_quotation_to_float() -> None:
    value = quotation_to_float({"units": "316", "nano": 210000000})
    assert value == 316.21


def test_select_best_instrument_prefers_exact_ticker_match() -> None:
    instruments = [
        InstrumentRef(
            symbol="SBER",
            instrument_uid="bond-uid",
            figi="TCS00A109X37",
            ticker="RU000A109X37",
            class_code="TQCB",
            instrument_type="bond",
            lot=1,
            name="Bond",
        ),
        InstrumentRef(
            symbol="SBER",
            instrument_uid="share-uid",
            figi="BBG004730N88",
            ticker="SBER",
            class_code="TQBR",
            instrument_type="share",
            lot=1,
            name="Sber share",
        ),
    ]

    selected = select_best_instrument("SBER", instruments)
    assert selected.instrument_uid == "share-uid"

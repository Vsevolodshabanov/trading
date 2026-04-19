"""Configuration constants for the MOEX momentum notebook on local T-Bank dataset."""

from pathlib import Path
import pandas as pd
import datetime as dt


DATASET_DIR = Path(__file__).resolve().parent.parent / "data_exports" / "ru_shares_2022_daily"

# Основные переменные
START_DATE = "2022-01-01"
BACKTEST_START = pd.Timestamp(START_DATE)
END_DATE = dt.datetime.today().date()
INITIAL_INVESTMENT = 1_000_000
BENCHMARK_TICKERS_LIST = ["SBER", "LKOH", "GAZP"]


# Strategy parameters
REBALANCE_FREQ = "W"
MOMENTUM_PERIODS = [30, 90, 126]
MOMENTUM_RANK = 95
CASH_RETURN_RATE = 0.00

# Data quality / universe controls
CALENDAR_WEEKDAYS_ONLY = True
CALENDAR_MIN_ACTIVE_COVERAGE_RATIO = 0.75
UNIVERSE_MIN_HISTORY_DAYS = max(MOMENTUM_PERIODS)
LIQUIDITY_WINDOW_DAYS = 63
MIN_MEDIAN_TURNOVER_RUB = 20_000_000.0
MAX_DAILY_RETURN = 1.00
MIN_DAILY_RETURN = -0.50

# Transaction cost parameters
TC_FIXED = 5.0
TC_PCT = 0.0004
LOT_SIZE = 1
MANAGEMENT_FEE_MONTHLY = 0.0

# =============================================================================
# REGIME FILTERS CONFIGURATION
# =============================================================================

# SPX/VIX filter is intentionally disabled for the local MOEX dataset notebook.
# We keep the same framework, but use local drawdown-based control instead.
REGIME_SPX_VIX_ENABLED = False
SPX_TICKER = "^GSPC"
VIX_TICKER = "^VIX"
SPX_MA_PERIOD = 200
VIX_THRESHOLD = 25

REGIME_DRAWDOWN_ENABLED = True
DRAWDOWN_WINDOW = 30
DRAWDOWN_THRESHOLD = -0.12
DRAWDOWN_SOURCE = "STRATEGY"

REGIME_VOLATILITY_ENABLED = False
VOLATILITY_WINDOW = 30
VOLATILITY_THRESHOLD = 0.45
VOLATILITY_SOURCE = "STRATEGY"

REGIME_FILTER_LOGIC = "any"


def _format_config_value(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def get_config_settings():
    """Return all uppercase config settings as a dict."""
    settings = {
        key: _format_config_value(value)
        for key, value in globals().items()
        if key.isupper() and not key.startswith("_")
    }
    return settings


def _iter_config_lines_in_order():
    """Yield tuples describing config lines in file order."""
    import os
    import re

    assignment_re = re.compile(r"^([A-Z][A-Z0-9_]*)\s*=")
    try:
        with open(os.path.abspath(__file__), "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("#"):
                    yield ("comment", stripped)
                    continue
                match = assignment_re.match(stripped)
                if match:
                    yield ("setting", match.group(1))
    except OSError:
        return


def print_config_settings():
    """Print all config settings in file order with comments/sections."""
    settings = get_config_settings()
    printed = set()
    print("=== CONFIG SETTINGS ===")

    first_line = True
    for kind, payload in _iter_config_lines_in_order() or []:
        if kind == "comment":
            is_block_header = payload.startswith("# ===") or payload.startswith("# ---") or payload.startswith("# REGIME")
            if not first_line and is_block_header:
                print("")
            print(payload)
            first_line = False
            continue
        if kind == "setting" and payload in settings:
            print(f"{payload}: {settings[payload]}")
            printed.add(payload)
            first_line = False

    remaining = [k for k in settings.keys() if k not in printed]
    if remaining:
        print("# Other settings")
        for key in remaining:
            print(f"{key}: {settings[key]}")

"""Configuration constants for the momentum strategy backtest."""

import pandas as pd
import datetime as dt


# Основные переменные
START_DATE = '2021-09-01'
BACKTEST_START = pd.Timestamp('2023-01-01 00:00:00')
END_DATE = dt.datetime.today().date()
INITIAL_INVESTMENT = 100000
BENCHMARK_TICKERS_LIST = ['SPY', 'SPLV', 'SPMO']


# Strategy parameters
REBALANCE_FREQ = 'W'  # 'W' for weekly, 'M' for monthly
MOMENTUM_PERIODS = [30, 90, 126]  # Lookback periods for momentum calculation (days). Can be single [30] or multi [30, 90, 126]
MOMENTUM_RANK = 95    # Percentile rank threshold for HQM selection (e.g., 95 = top 5%)
CASH_RETURN_RATE = 0.04  # Annual return rate when in cash (e.g., 0.04 = 4%)

# Transaction cost parameters (for execution-based backtest)
TC_FIXED = 1.0        # Fixed cost per order (USD)
TC_PCT = 0.0005       # Proportional cost as fraction (0.0005 = 5 basis points = 0.05%)
LOT_SIZE = 1          # Minimum tradeable unit (shares)
MANAGEMENT_FEE_MONTHLY = 0.001  # Monthly management fee as fraction (0.001 = 0.1% per month = 1.2% per year)

# =============================================================================
# REGIME FILTERS CONFIGURATION
# =============================================================================

# --- SPX/VIX Filter (market regime) ---
# Signal ON when: SPX > MA AND VIX < threshold
REGIME_SPX_VIX_ENABLED = True
SPX_TICKER = '^GSPC'           # SPX index ticker
VIX_TICKER = '^VIX'            # VIX index ticker
SPX_MA_PERIOD = 200            # Moving average period in days
VIX_THRESHOLD = 25             # VIX threshold (ON when VIX < threshold)

# --- Drawdown Filter ---
# Signal ON when: rolling drawdown > threshold (less negative)
# Can be based on STRATEGY or any benchmark (SPY, SPMO, etc.)
REGIME_DRAWDOWN_ENABLED = False
DRAWDOWN_WINDOW = 30           # Rolling window in days
DRAWDOWN_THRESHOLD = -0.10     # -10% threshold (OFF when DD < -10%)
DRAWDOWN_SOURCE = 'SPY'   # 'STRATEGY' | 'SPY' | 'SPMO' | etc.

# --- Volatility Filter ---
# Signal ON when: rolling volatility < threshold
# Can be based on STRATEGY or any benchmark
REGIME_VOLATILITY_ENABLED = False
VOLATILITY_WINDOW = 30         # Rolling window in days
VOLATILITY_THRESHOLD = 0.40    # 40% annualized (OFF when vol > 40%)
VOLATILITY_SOURCE = 'SPY' # 'STRATEGY' | 'SPY' | 'SPMO' | etc.

# --- Filter Combination Logic ---
# 'any' = OFF if ANY filter triggers (strict, more protective)
# 'all' = OFF only if ALL filters trigger (lenient)
REGIME_FILTER_LOGIC = 'any'


def _format_config_value(value):
	if isinstance(value, pd.Timestamp):
		return value.strftime('%Y-%m-%d')
	if isinstance(value, dt.datetime):
		return value.strftime('%Y-%m-%d %H:%M:%S')
	return value


def get_config_settings():
	"""Return all uppercase config settings as a dict."""
	settings = {
		key: _format_config_value(value)
		for key, value in globals().items()
		if key.isupper() and not key.startswith('_')
	}
	return settings


def _iter_config_lines_in_order():
	"""Yield tuples describing config lines in file order.

	Returns tuples of:
	- ("comment", text) for comment/section lines
	- ("setting", key) for uppercase setting assignments
	"""
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

	# Fallback for any settings not present in file order
	remaining = [k for k in settings.keys() if k not in printed]
	if remaining:
		print("# Other settings")
		for key in remaining:
			print(f"{key}: {settings[key]}")

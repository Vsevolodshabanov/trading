"""Data loading functions for fetching price data and tickers."""

import pandas as pd
import numpy as np
import yfinance as yf
from config import START_DATE, END_DATE


def load_tickers(file_name):
    """Загрузка списка тикеров из файла от Bloomberg и их корректировка для yahoo finance"""
    ticks = pd.read_excel(file_name)
    tickers = ticks['Ticker'].str.replace('/', '-').tolist()
    return tickers


def fetch_price_data_divs(tickers, start=START_DATE, end=END_DATE, batch_size=100):
    """Fetch historical price data in batches for efficiency.

    Returns Open prices (for realistic execution at market open).
    """
    pricing_table = pd.DataFrame()
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        prices = yf.Tickers(batch).download(start=start, end=end)
        pricing_table = pricing_table.join(prices, how='outer') if not pricing_table.empty else prices
    price_table = pricing_table['Open']
    price_table_with_divs = pricing_table['Open'].add(pricing_table['Dividends'].groupby(pd.Grouper(freq='YE')).cumsum())
    divs = pricing_table['Dividends'].groupby(pd.Grouper(freq='YE')).cumsum()
    return price_table, price_table_with_divs, divs


def get_benchmark_prices(benchmark_tickers_list, start=START_DATE, end=END_DATE):
    """Fetch benchmark Open price data."""
    benchmark_tickers = yf.Tickers(benchmark_tickers_list)
    benchmark_prices = benchmark_tickers.history(start=start, end=end)['Open']
    return benchmark_prices

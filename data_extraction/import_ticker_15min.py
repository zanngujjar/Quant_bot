#!/usr/bin/env python3
"""
import_ticker_15min.py

Fetches 15-minute aggregate bars for all tickers from Polygon.io for the past 3 years.
Requires:
    - Python 3.9+
    - Requests library
    - python-dateutil library
    - tqdm library for progress bar
    - POLYGON_API_KEY environment variable set to your Polygon API key
"""
import sys
import os
import requests
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# Base URL for 15-minute bars
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/{from_date}/{to_date}"

# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)
API_KEY = os.getenv("POLYGON_API_KEY")

def fetch_15min_bars(ticker: str, from_date: str, to_date: str, api_key: str = None) -> list:
    """
    Fetches 15-minute aggregate bars for `ticker` between `from_date` and `to_date` (inclusive).
    :param ticker: e.g. "SPY"
    :param from_date: YYYY-MM-DD
    :param to_date: YYYY-MM-DD
    :param api_key: Polygon API key; if None, reads POLYGON_API_KEY
    :return: List of dicts with bar data and converted NY date/time
    """
    key = api_key or API_KEY
    if not key:
        raise ValueError("No API key provided. Set POLYGON_API_KEY environment variable or pass api_key parameter.")

    url = BASE_URL.format(ticker=ticker, from_date=from_date, to_date=to_date)
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": key,
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    bars = []
    for bar in data.get("results", []):
        ms = bar["t"]  # Unix ms timestamp at window start
        # Convert to New York local time
        dt_utc = datetime.fromtimestamp(ms / 1000, tz=ZoneInfo("UTC"))
        dt_ny = dt_utc.astimezone(ZoneInfo("America/New_York"))
        
        # Calculate nanosecond timestamp
        nanosecond = int(ms * 1_000_000)  # Convert ms to nanoseconds

        bars.append({
            "ticker": ticker,
            "bar_date": dt_ny.date().isoformat(),
            "bar_time": dt_ny.time().isoformat(timespec="seconds"),
            "open": bar["o"],
            "high": bar["h"],
            "low": bar["l"],
            "close": bar["c"],
            "volume": bar["v"],
            "nanosecond": nanosecond
        })
    return bars

def process_all_tickers():
    """
    Process all tickers from the database and fetch their 15-minute bars.
    """
    # Calculate date range for the past 3 years
    today = date.today()
    from_date = (today - relativedelta(years=3)).isoformat()
    to_date = today.isoformat()

    with Database() as db:
        # Get all tickers from the database
        tickers = db.get_all_tickers()
        
        print(f"Found {len(tickers)} tickers in database")
        
        # Initialize list to store all records
        all_records = []
        total_bars = 0
        
        # Process each ticker with progress bar
        for ticker_id, ticker_symbol in tqdm(tickers, desc="Processing tickers"):
            try:
                bars = fetch_15min_bars(ticker_symbol, from_date, to_date)
                total_bars += len(bars)
                
                # Prepare data for batch insert
                for bar in bars:
                    record = (
                        ticker_id,
                        ticker_symbol,
                        bar['bar_date'],
                        bar['bar_time'],
                        float(bar['open']),
                        float(bar['high']),
                        float(bar['low']),
                        float(bar['close']),
                        int(bar['volume']),
                        bar['nanosecond']
                    )
                    all_records.append(record)
                
            except Exception as e:
                print(f"\nError processing {ticker_symbol}: {e}")
        
        # Store all records in database at once
        if all_records:
            print(f"\nStoring {len(all_records)} total bars in database...")
            db.add_15min_prices_batch(all_records)
            print("Data import completed successfully!")

if __name__ == "__main__":
    process_all_tickers()

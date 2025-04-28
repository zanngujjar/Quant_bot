import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

API_KEY  = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"

def fetch_new_price_data(ticker: str, last_date_str: str, cutoff_date_str: str) -> pd.DataFrame:
    """
    Pulls daily OHLCV for `ticker` from (last_date + 1 day) up to cutoff_date.
    E.g. last_date_str="2025-04-20", cutoff_date_str="2025-04-28"
    """
    # parse strings
    last_date   = datetime.strptime(last_date_str, "%Y-%m-%d").date()
    cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()

    # start the day after your last stored date
    start_date = last_date + timedelta(days=1)
    if start_date > cutoff_date:
        # nothing new to pull
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])

    endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{cutoff_date}"
    url      = BASE_URL + endpoint

    params = {
        "adjusted": "true",
        "sort":     "asc",
        "limit":    5000,
        "apiKey":   API_KEY
    }

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("results"):
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])

    rows = []
    for r in data["results"]:
        date = pd.to_datetime(r["t"], unit="ms").date()
        rows.append({
            "date": date,
            "open": r["o"],
            "high": r["h"],
            "low": r["l"],
            "close": r["c"],
            "volume": r["v"],
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("date")
    return df

def update_all_ticker_prices():
    """Update price data for all tickers in the database"""
    try:
        with Database() as db:
            # Get latest dates for all tickers
            latest_dates = db.get_latest_price_dates()
            if not latest_dates:
                print("No tickers found in the database")
                return
            
            # Get today's date as cutoff
            today = datetime.now().date()
            today_str = today.strftime("%Y-%m-%d")
            
            # Process each ticker
            for ticker, last_date in latest_dates:
                print(f"\nProcessing {ticker}...")
                print(f"Last date in database: {last_date}")
                
                try:
                    # Fetch new price data
                    new_data = fetch_new_price_data(ticker, last_date, today_str)
                    
                    if new_data.empty:
                        print(f"No new data available for {ticker}")
                        continue
                    
                    print(f"Fetched {len(new_data)} new price points for {ticker}")
                    
                    # Get ticker ID
                    ticker_id = db.get_ticker_id(ticker)
                    if not ticker_id:
                        print(f"Error: Ticker {ticker} not found in database")
                        continue
                    
                    # Insert new price data
                    for _, row in new_data.iterrows():
                        db.add_ticker_price(
                            ticker_id=ticker_id,
                            date=row['date'].strftime("%Y-%m-%d"),
                            close_price=row['close']
                        )
                    
                    print(f"Successfully updated {ticker} with new price data")
                    
                except Exception as e:
                    print(f"Error processing {ticker}: {e}")
                    continue
            
            print("\nPrice update completed!")
            
    except Exception as e:
        print(f"Error updating prices: {e}")
        raise

# ── Example usage ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    update_all_ticker_prices()

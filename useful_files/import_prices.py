import requests
import pandas as pd
from datetime import date, timedelta
from tqdm import tqdm
from DB.database import Database

# Polygon.io API configuration
BASE_URL = "https://api.polygon.io"
API_KEY = "hviobeGfrAoWxLm4Z7uB12xMBT3j6pOT"  # Replace with your actual API key

def fetch_5y_history(ticker: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV for the past 5 years for `ticker`.
    Returns a DataFrame with columns: date, close.
    """
    # Polygon date strings: YYYY-MM-DD
    end_date   = date.today()
    start_date = end_date - timedelta(days=5*365)
    endpoint   = f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    url        = BASE_URL + endpoint

    params = {
        "adjusted": "true",
        "sort":     "asc",
        "limit":    50000,       # more than enough for ~1,260 trading days
        "apiKey":   API_KEY
    }

    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if "results" not in data:
            print(f"No data for {ticker}: {data}")
            return pd.DataFrame()

        records = []
        for r in data["results"]:
            records.append({
                "date":   pd.to_datetime(r["t"], unit="ms").date(),
                "close":  r["c"],
            })

        df = pd.DataFrame(records)
        df.set_index("date", inplace=True)
        return df
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

def import_prices():
    """Import price data for all tickers in the database"""
    try:
        with Database() as db:
            # Get all tickers from the database
            db.cursor.execute("SELECT id, symbol FROM tickers")
            tickers = db.cursor.fetchall()
            
            if not tickers:
                print("No tickers found in the database. Please import tickers first.")
                return
            
            print(f"Found {len(tickers)} tickers to process")
            
            # Process each ticker
            for ticker_id, symbol in tqdm(tickers, desc="Processing tickers"):
                # Fetch price data
                df = fetch_5y_history(symbol)
                
                if df.empty:
                    print(f"Skipping {symbol} - no data available")
                    continue
                
                # Prepare batch insert
                batch = []
                for date, row in df.iterrows():
                    batch.append((ticker_id, date.strftime('%Y-%m-%d'), row['close']))
                
                # Insert in batches of 1000
                batch_size = 1000
                for i in range(0, len(batch), batch_size):
                    current_batch = batch[i:i + batch_size]
                    db.cursor.executemany("""
                        INSERT OR REPLACE INTO ticker_prices 
                        (ticker_id, date, close_price) 
                        VALUES (?, ?, ?)
                    """, current_batch)
                    db.conn.commit()
                
                print(f"Imported {len(batch)} price points for {symbol}")
            
            print("\nPrice import completed successfully!")
            
    except Exception as e:
        print(f"Error importing prices: {e}")
        raise

if __name__ == "__main__":
    import_prices()

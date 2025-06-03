import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
import pandas as pd
from typing import List, Dict, Tuple
from tqdm import tqdm

def extract_all_log_prices() -> pd.DataFrame:
    """
    Extract all ticker log prices from the database and return as a DataFrame.
    
    Returns:
        DataFrame with columns: ticker_id, date, log_price
    """
    with Database() as db:
        # Get all ticker IDs
        ticker_ids = db.get_all_ticker_ids()
        
        # Initialize lists to store data
        all_ticker_ids = []
        all_dates = []
        all_log_prices = []
        
        # Process each ticker
        for ticker_id in tqdm(ticker_ids, desc="Processing tickers"):
            # Get all price IDs and dates for this ticker
            price_data = db.get_ticker_price_ids(ticker_id)
            
            if not price_data:
                continue
                
            # Extract price IDs
            price_ids = [pid for pid, _ in price_data]
            
            # Get log prices for all price IDs
            log_prices = db.get_log_price_ids_batch(price_ids)
            
            # Add data to lists
            for (price_id, date) in price_data:
                if price_id in log_prices:
                    all_ticker_ids.append(ticker_id)
                    all_dates.append(date)
                    all_log_prices.append(log_prices[price_id])
        
        # Create DataFrame
        df = pd.DataFrame({
            'ticker_id': all_ticker_ids,
            'date': all_dates,
            'log_price': all_log_prices
        })
        
        return df

def main():
    # Extract data
    print("Extracting log prices from database...")
    df = extract_all_log_prices()
    
    # Save to parquet
    output_file = "log_prices.parquet"
    print(f"Saving to {output_file}...")
    df.to_parquet(output_file, index=False)
    print("Done!")

if __name__ == "__main__":
    main()

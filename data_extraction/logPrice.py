import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

def calculate_log_prices():
    """Calculate log prices and rolling statistics for tickers with missing log prices"""
    try:
        with Database() as db:
            # Get all price records that need log price calculation
            missing_prices = db.get_missing_log_prices()
            
            if not missing_prices:
                print("All price records already have log prices calculated.")
                return
            
            print(f"Found {len(missing_prices)} price records needing log price calculation")
            
            # Group by ticker to process each ticker's data together
            ticker_data = {}
            for price_id, symbol, date, close_price in missing_prices:
                if symbol not in ticker_data:
                    ticker_data[symbol] = []
                ticker_data[symbol].append((price_id, date, close_price))
            
            # Process each ticker's data
            for symbol, price_records in tqdm(ticker_data.items(), desc="Processing tickers"):
                # Convert to DataFrame
                df = pd.DataFrame(price_records, columns=['price_id', 'date', 'close'])
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)  # Ensure dates are in order
                
                # Calculate log prices
                df['log_price'] = np.log(df['close'])
                
                # Calculate rolling statistics
                df['mean_30d'] = df['log_price'].rolling(window=30, min_periods=1).mean()
                df['std_30d'] = df['log_price'].rolling(window=30, min_periods=1).std()
                df['mean_90d'] = df['log_price'].rolling(window=90, min_periods=1).mean()
                df['std_90d'] = df['log_price'].rolling(window=90, min_periods=1).std()
                
                # Prepare batch insert
                batch = []
                for idx, row in df.iterrows():
                    batch.append((
                        row['price_id'],  # This will be used as both id and ticker_price_id
                        row['price_id'],  # ticker_price_id
                        row['log_price'],
                        row['mean_30d'],
                        row['std_30d'],
                        row['mean_90d'],
                        row['std_90d']
                    ))
                
                # Insert in batches of 1000
                batch_size = 1000
                for i in range(0, len(batch), batch_size):
                    current_batch = batch[i:i + batch_size]
                    db.cursor.executemany("""
                        INSERT OR REPLACE INTO log_prices 
                        (id, ticker_price_id, log_price, mean_30d, std_30d, mean_90d, std_90d) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, current_batch)
                    db.conn.commit()
                
                print(f"Processed {len(batch)} log price points for {symbol}")
            
            print("\nLog price calculation completed successfully!")
            
    except Exception as e:
        print(f"Error calculating log prices: {e}")
        raise

if __name__ == "__main__":
    calculate_log_prices()

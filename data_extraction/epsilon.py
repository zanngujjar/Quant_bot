import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import matplotlib.pyplot as plt
from DB.database import Database
import pandas as pd
import numpy as np
from matplotlib.dates import DayLocator

# Initialize database connection and ensure proper cleanup
db = Database()
db.connect()

try:
    cointegrated_data = db.get_latest_cointegrated_pairs()
    
    # Split the data into pairs and betas
    cointegrated_pairs = [(pair[0], pair[1]) for pair in cointegrated_data]
    betas = {(pair[0], pair[1]): pair[2] for pair in cointegrated_data}
    alphas = {(pair[0], pair[1]): pair[3] for pair in cointegrated_data}
    
    # Get log prices for all cointegrated pairs (180 days)
    pair_prices = db.get_latest_log_prices_for_pairs(cointegrated_pairs, days=180)

    # Process the pair_prices data
    results = []
    epsilon_data = []  # List to store formatted data for batch upload
        
    for i, (pair, prices) in enumerate(pair_prices.items()):
        print(f"Processing pair {i+1}/{len(pair_prices)}")
        
        # Convert the list of tuples/lists to a DataFrame
        df = pd.DataFrame(prices, columns=['date', pair[0], pair[1]])
        
        # Get the beta for this pair
        beta = betas[pair]
        alpha = alphas[pair]

        # Ensure date is datetime and sorted
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Calculate spread: spread = A - alpha - βB
        df['spread'] = df[pair[0]] - alpha - beta * df[pair[1]]
        
        # Initialize result columns
        df['rolling_mean'] = np.nan
        df['rolling_std'] = np.nan
        df['zscore'] = np.nan
        df['epsilon'] = np.nan
        
        # For days 90 onward (day 91 = index 90)
        for t in range(90, len(df)):
            window = df.iloc[t-90:t]  # previous 90 days
            
            # Calculate rolling mean and std
            mean = window['spread'].mean()
            std = window['spread'].std()
            
            current_spread = df.iloc[t]['spread']
            z = (current_spread - mean) / std if std != 0 else 0
            
            df.at[df.index[t], 'rolling_mean'] = mean
            df.at[df.index[t], 'rolling_std'] = std
            df.at[df.index[t], 'zscore'] = z
            df.at[df.index[t], 'epsilon'] = abs(z)
        
        # Calculate z-score
        df['zscore'] = (df['spread'] - df['rolling_mean']) / df['rolling_std']
        
        # Get ticker IDs
        ticker1_id = db.get_ticker_id(pair[0])
        ticker2_id = db.get_ticker_id(pair[1])
        
        # Format data for batch upload
        for idx, row in df.iterrows():
            if pd.notna(row['rolling_mean']):  # Only include rows with valid calculations
                date_str = row['date'].strftime('%Y-%m-%d')
                
                # First get ticker_price_ids using ticker_id and date
                ticker_price1_id = db.get_ticker_price_id(ticker1_id, date_str)
                ticker_price2_id = db.get_ticker_price_id(ticker2_id, date_str)
                
                # Then get log_price_ids from ticker_price_ids
                if ticker_price1_id and ticker_price2_id:
                    price1_id = db.get_log_price_id_from_ticker_price(ticker_price1_id)
                    price2_id = db.get_log_price_id_from_ticker_price(ticker_price2_id)
                    
                    if price1_id and price2_id:  # Only include if we have both log price IDs
                        epsilon_data.append((
                            price1_id,           # ticker_1_logprice_id
                            price2_id,           # ticker_2_logprice_id
                            ticker1_id,          # ticker_id_1
                            ticker2_id,          # ticker_id_2
                            row['epsilon'],      # epsilon
                            row['rolling_mean'], # rolling_mean
                            row['rolling_std'],  # rolling_std
                            row['zscore'],       # z_score
                            date_str             # date
                        ))
    
    # Batch upload the epsilon data
    if epsilon_data:
        print(f"Uploading {len(epsilon_data)} epsilon price records...")
        db.add_epsilon_prices_batch(epsilon_data)
        print("Upload complete!")
    
    print("done")

finally:
    # Ensure database connection is closed even if an error occurs
    db.close()
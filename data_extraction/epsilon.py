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
    
    # Take only the first pair
    cointegrated_data = [cointegrated_data[27439]]  # Only process first pair
    
    # Split the data into pairs and betas
    cointegrated_pairs = [(pair[0], pair[1]) for pair in cointegrated_data]
    betas = {(pair[0], pair[1]): pair[2] for pair in cointegrated_data}
    
    # Get log prices for all cointegrated pairs (180 days)
    pair_prices = db.get_latest_log_prices_for_pairs(cointegrated_pairs, days=180)

    # Process the pair_prices data
    results = []
    for i, (pair, prices) in enumerate(pair_prices.items()):
        print(f"Processing pair {i+1}/{len(pair_prices)}")
        
        # Convert the list of tuples/lists to a DataFrame
        df = pd.DataFrame(prices, columns=['date', pair[0], pair[1]])
        
        # Get the beta for this pair
        beta = betas[pair]
        
        # Ensure date is datetime and sorted
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Calculate spread: spread = A - βB
        df['spread'] = df[pair[0]] - beta * df[pair[1]]
        
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
        
        results.append(df)

    # Plotting code
    pairs_list = list(pair_prices.keys())
    for i, df in enumerate(results):
        if len(df) < 90:
            continue  # Skip if not enough data

        # First subplot: log prices
        plt.figure(figsize=(14, 6))
        plt.plot(df['date'], df[pairs_list[i][0]], label=f"{pairs_list[i][0]} (log price)")
        plt.plot(df['date'], df[pairs_list[i][1]], label=f"{pairs_list[i][1]} (log price)")
        plt.title(f"Log Prices for {pairs_list[i][0]} and {pairs_list[i][1]}")
        plt.legend()
        plt.grid(True)
        plt.gca().xaxis.set_major_locator(DayLocator(interval=7))  # Show weekly ticks
        plt.gcf().autofmt_xdate()
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

        # Second subplot: spread, rolling mean, and z-score bands
        plt.figure(figsize=(14, 6))
        plt.plot(df['date'], df['spread'], label='Spread', color='gray')
        plt.plot(df['date'], df['rolling_mean'], label='Rolling Mean', color='blue')

        # Plot z-score ±2 lines
        plt.plot(df['date'], df['rolling_mean'] + 2 * df['rolling_std'], label='+2σ Band', linestyle='--', color='green')
        plt.plot(df['date'], df['rolling_mean'] - 2 * df['rolling_std'], label='-2σ Band', linestyle='--', color='red')

        plt.title(f"Spread and Rolling Bands for {pairs_list[i][0]}-{pairs_list[i][1]}")
        plt.legend()
        plt.grid(True)
        plt.gca().xaxis.set_major_locator(DayLocator(interval=7))  # Show weekly ticks
        plt.gcf().autofmt_xdate()
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

        break

finally:
    # Ensure database connection is closed even if an error occurs
    db.close()
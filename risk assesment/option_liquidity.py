#!/usr/bin/env python3
# option_liquidity.py

import os
import sys
import pandas as pd
import requests
from dotenv import load_dotenv   # pip install python-dotenv

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# Import option_tables.py (since it's in the same directory)
from option_tables import *
from option_IV_test import *
# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

# Get API key from environment variables
API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    sys.exit("❌  POLYGON_API_KEY not found in .env")
# Read in filtered ticker data
try:
    filtered_data = pd.read_parquet("filtered_ticker_dates.parquet")
except FileNotFoundError:
    sys.exit("❌ filtered_ticker_dates.parquet not found")
except Exception as e:
    sys.exit(f"❌ Error reading parquet file: {e}")

# Initialize database connection
db = Database()
option_greeks = []
with db:
    #get tickers from database
    tickers = db.get_tickers()

    for ticker in tickers:
        #get close price from database
        if ticker[0] not in filtered_data['ticker_id'].values:
            continue
        close_price = db.get_ticker_prices(ticker[0])
        for price in close_price: 
            if price[0] not in filtered_data[filtered_data['ticker_id'] == ticker[0]]['date'].values:
                continue
            #get optimal expiry date for the ticker on a given day
            exp_date = first_expiry_in_band(ticker[1], price[0])
            #if no expiry date is found, continue to the next date
            if exp_date is None:
                continue
            #based on expirey date, get option chain based upon close price
            urls = {
                contract_query(ticker[1], price[1], "call", price[0], exp_date),
                contract_query(ticker[1], price[1], "put", price[0], exp_date)
            }
            #for each call and put option chain
            for url in urls:
                response = requests.get(url)
                if response.status_code == 200:
                    option_contracts = response.json()
                    if 'results' in option_contracts and option_contracts['results']:
                        # Extract strike_price and option ticker, contract type, and contract info for each contract
                        contracts_data = []
                        for contract in option_contracts['results']:
                            contract_info = {
                                'ticker': contract.get('ticker'),
                                'strike_price': contract.get('strike_price'),
                                'contract_type': contract.get('contract_type'),
                            }
                            contracts_data.append(contract_info)
                        
                        # Convert to DataFrame for easier handling
                        option_stats = pd.DataFrame(contracts_data)
                        # Sort options by strike price to ensure we process them in order
                        option_stats = option_stats.sort_values('strike_price')
                        
                        # Initialize variables for delta targeting
                        target_delta = 0.35
                        best_option = None
                        best_delta_diff = float('inf')
                        
                        #for each option in the given option chain
                        for index, option in option_stats.iterrows():
                            #extract the option p_obs from the polygon api based on close price 
                            #and the last 5 minutes of trade time 
                            P_obs = get_price_obs(option['ticker'], price[0])
                            #if the option p_obs is found, and the option chain is not empty
                            if P_obs and 'results' in P_obs and P_obs['results']:
                                #extract the option p_obs from the option chain
                                trade_data = P_obs['results'][0]
                                P_obs = trade_data['price']
                                nanosecond = int(trade_data['participant_timestamp'])  # Convert to int
                                #if the option is a call, set cp to 1, otherwise set it to -1
                                cp = 1 if option['contract_type'] == 'call' else -1
                                #inputs: S, K, P_obs, cp, entry, exit, ticker_underlying
                                iv, delta, gamma, theta, vega = get_greeks(price[1], option['strike_price'], P_obs, cp, price[0], exp_date, ticker[1])
                                option_greeks.append([ticker[1], option['ticker'], price[1], option['strike_price'], P_obs, cp, price[0], exp_date, iv, delta, gamma, theta, vega, nanosecond])
                                # Calculate how close this delta is to our target
                                delta_diff = abs(delta - target_delta)
                                
                                # If this option is closer to our target than any we've seen before
                                if delta_diff < best_delta_diff:
                                    print(f"Found better option with ticker: {option['ticker']}")  # Debug print
                                    best_option = {
                                        'ticker_underlying': ticker[1],
                                        'ticker_option': option['ticker'],
                                        'S': price[1],
                                        'K': option['strike_price'],
                                        'P_obs': P_obs,
                                        'cp': cp,
                                        'entry': price[0],
                                        'expire': exp_date,
                                        'iv': iv,
                                        'delta': delta,
                                        'gamma': gamma,
                                        'theta': theta,
                                        'vega': vega,
                                        'nanosecond': nanosecond
                                    }
                                    best_delta_diff = delta_diff
                        
                        # If we found a suitable option, add it to our results
                        if best_option is not None:
                            print(f"Selected option ticker: {best_option['ticker_option']}")  # Debug print
                            print(f"Entry: {best_option['entry']}")
                            # Get volume statistics for the selected option
                            vol_stats = get_option_vol(best_option['ticker_option'], best_option['entry'])
                            if vol_stats and 'results' in vol_stats and vol_stats['results']:
                                print(f"Volume: {vol_stats['results'][0]['v']}")
                                print(f"VWAP: {vol_stats['results'][0]['vw']}")
                                print(f"Number of trades: {vol_stats['results'][0]['n']}")
                                print(f"High: {vol_stats['results'][0]['h']}")
                                print(f"Low: {vol_stats['results'][0]['l']}")
                                print(f"Close: {best_option['P_obs']}")
                                print(f"Option Ticker: {best_option['ticker_option']}")  # Print the actual ticker value
                            quote_data = get_option_quotes(best_option['ticker_option'], best_option['entry'])
                            if quote_data and 'results' in quote_data and quote_data['results']:
                                quote = quote_data['results'][0]
                                slippage_stats = {
                                    'bid_price': quote['bid_price'],
                                    'bid_size': quote['bid_size'], 
                                    'ask_price': quote['ask_price'],
                                    'ask_size': quote['ask_size'],
                                    'bid_ask_spread': quote['ask_price'] - quote['bid_price'],
                                    'mid_price': (quote['ask_price'] + quote['bid_price']) / 2,
                                    'relative_spread': (quote['ask_price'] - quote['bid_price']) / ((quote['ask_price'] + quote['bid_price']) / 2)
                                }
                                print(f"Bid-Ask Spread: ${slippage_stats['bid_ask_spread']:.4f}")
                                print(f"Relative Spread: {slippage_stats['relative_spread']*100:.2f}%")                                    print(f"Bid Size: {slippage_stats['bid_size']}")
                                print(f"Ask Size: {slippage_stats['ask_size']}")
# After processing all tickers, create DataFrame and save to parquet
if option_greeks:
    # Create DataFrame with specified columns
    df = pd.DataFrame(option_greeks, columns=[
        'ticker_underlying', 'ticker_option', 'S', 'K', 'P_obs', 'cp', 'entry', 'expire',
        'iv', 'delta', 'gamma', 'theta', 'vega', 'nanosecond'
    ])
    
    # Save to parquet file
    output_file = "option_greeks.parquet"
    df.to_parquet(output_file)
    print(f"\nSaved {len(df)} option greeks records to {output_file}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Number of unique tickers: {df['ticker_underlying'].nunique()}")
    print(f"Date range: {df['entry'].min()} to {df['entry'].max()}")
    print(f"Number of calls: {(df['cp'] == 1).sum()}")
    print(f"Number of puts: {(df['cp'] == -1).sum()}")
    print(f"Average delta: {df['delta'].mean():.4f}")
    print(f"Delta range: {df['delta'].min():.4f} to {df['delta'].max():.4f}")
else:
    print("No option greeks data was collected.")

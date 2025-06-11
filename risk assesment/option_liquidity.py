#!/usr/bin/env python3
# option_liquidity.py

import os
import sys
import pandas as pd
import requests
from dotenv import load_dotenv   # pip install python-dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import multiprocessing
from tqdm import tqdm
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    filtered_data = pd.read_parquet("option_liquidity_filtered.parquet")
except FileNotFoundError:
    sys.exit("❌ option_liquidity.parquet not found")
except Exception as e:
    sys.exit(f"❌ Error reading parquet file: {e}")
filtered_data = filtered_data.sort_values('entry_date', ascending=False)

def create_session():
    """Create a requests session with connection pooling and retry strategy"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Mount adapter with connection pooling
    adapter = HTTPAdapter(
        pool_connections=20,  # Reduced from 100
        pool_maxsize=20,      # Reduced from 100
        max_retries=retry_strategy
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def process_ticker(ticker, ticker_filtered_data):
    """Process a single ticker and return option_greeks and liquidity_data"""
    ticker_option_greeks = []
    ticker_liquidity_data = []
    
    # Create a session for this thread
    session = create_session()
    
    # Create a new database connection for this thread
    db = Database()
    with db:
        close_price = db.get_ticker_prices_dec(ticker[0])
        for price in close_price: 
            if price[0] not in ticker_filtered_data['entry_date'].values:
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
                # Add small delay to prevent overwhelming API and socket exhaustion
                time.sleep(0.1)  # 100ms delay between requests
                response = session.get(url, timeout=30)
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
                            P_obs = get_price_obs(option['ticker'], price[0], session)
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
                                ticker_option_greeks.append([ticker[1], option['ticker'], price[1], option['strike_price'], P_obs, cp, price[0], exp_date, iv, delta, gamma, theta, vega, nanosecond])
                                # Calculate how close this delta is to our target
                                delta_diff = abs(delta - target_delta)
                                
                                # If this option is closer to our target than any we've seen before
                                if delta_diff < best_delta_diff:
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
                            # Get volume statistics for the selected option
                            vol_stats = get_option_vol(best_option['ticker_option'], best_option['entry'], session)
                            if vol_stats['results'][0] is None:
                                continue
                            q = get_option_quotes(best_option['ticker_option'], best_option['entry'], session)
                            if q and 'results' in q and q['results']:
                                quote = q['results'][0]
                                slippage_stats = {
                                    'bid_price': quote['bid_price'],
                                    'bid_size': quote['bid_size'], 
                                    'ask_price': quote['ask_price'],
                                    'ask_size': quote['ask_size'],
                                    'bid_ask_spread': quote['ask_price'] - quote['bid_price'],
                                    'mid_price': (quote['ask_price'] + quote['bid_price']) / 2,
                                    'relative_spread': (quote['ask_price'] - quote['bid_price']) / ((quote['ask_price'] + quote['bid_price']) / 2)
                                }

                                hloc = vol_stats['results'][0]
                                if is_older_than_years(best_option['entry'], years=3):
                                    # calibrate on recent window
                                    factor = calibrate_spread_factor(q, { 'h': hloc['h'], 'l': hloc['l'] })
                                    mid    = (quote['ask_price'] + quote['bid_price'])/2
                                    est_sp = estimate_spread({ 'h':hloc['h'], 'l':hloc['l'] }, factor, mid)
                                    bid_ask_spread   = est_sp
                                    relative_spread  = est_sp / mid if mid else None
                                else:
                                    bid_ask_spread  = quote['ask_price'] - quote['bid_price']
                                    mid             = (quote['ask_price'] + quote['bid_price'])/2
                                    relative_spread = bid_ask_spread / mid if mid else None
                                volume = hloc.get('v', 0)
                                slip = relative_spread or 0
                                # === Liquidity filter & console output ===
                                if volume > 100 and slip <= 0.06 :  # 6% spread for high volume
                                    ticker_liquidity_data.append([ticker[0], best_option['entry']])
                                elif volume > 300 and slip <= 0.10:  # 10% spread for medium volume  
                                    ticker_liquidity_data.append([ticker[0], best_option['entry']])
                                else:
                                    continue
    
    # Close the session when done
    session.close()
    return ticker_option_greeks, ticker_liquidity_data

# Initialize database connection to get tickers
db = Database()
option_greeks = []
liquidity_data = []

with db:
    #get tickers from database
    tickers = db.get_tickers()

# Filter tickers that exist in filtered_data
valid_tickers = [ticker for ticker in tickers if ticker[0] in filtered_data['ticker_id'].values]

# Calculate optimal thread count - reduce to prevent socket exhaustion
optimal_threads = min(len(valid_tickers), max(4, multiprocessing.cpu_count()))  # Conservative: max CPU cores, min 4

print(f"🚀 Processing {len(valid_tickers)} tickers using {optimal_threads} threads")
print("📊 Starting option liquidity analysis...")

# Use ThreadPoolExecutor to process tickers in parallel
with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
    # Prepare futures for each ticker
    future_to_ticker = {}
    for ticker in valid_tickers:
        # Get filtered data for this specific ticker
        ticker_filtered_data = filtered_data[filtered_data['ticker_id'] == ticker[0]]
        future = executor.submit(process_ticker, ticker, ticker_filtered_data)
        future_to_ticker[future] = ticker
    
    # Initialize progress bar
    progress_bar = tqdm(
        total=len(valid_tickers),
        desc="Processing tickers",
        unit="ticker",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )
    
    # Collect results as they complete
    completed_count = 0
    successful_tickers = 0
    failed_tickers = 0
    total_greeks = 0
    total_liquidity = 0
    
    for future in as_completed(future_to_ticker):
        ticker = future_to_ticker[future]
        try:
            ticker_option_greeks, ticker_liquidity_data = future.result()
            # Thread-safe appending of results
            option_greeks.extend(ticker_option_greeks)
            liquidity_data.extend(ticker_liquidity_data)
            
            # Update counters
            successful_tickers += 1
            total_greeks += len(ticker_option_greeks)
            total_liquidity += len(ticker_liquidity_data)
            
            # Update progress bar with detailed info
            progress_bar.set_postfix({
                'Current': ticker[1][:6],
                'Greeks': len(ticker_option_greeks),
                'Liquidity': len(ticker_liquidity_data),
                'Success': successful_tickers,
                'Failed': failed_tickers
            })
            
        except Exception as exc:
            failed_tickers += 1
            progress_bar.set_postfix({
                'Current': f"{ticker[1][:6]}❌",
                'Success': successful_tickers,
                'Failed': failed_tickers,
                'Error': str(exc)[:20]
            })
        
        completed_count += 1
        progress_bar.update(1)
    
    # Close progress bar
    progress_bar.close()
    
    # Print final summary
    print(f"\n✅ Processing completed!")
    print(f"📈 Successfully processed: {successful_tickers}/{len(valid_tickers)} tickers")
    print(f"❌ Failed: {failed_tickers} tickers")
    print(f"🎯 Total option Greeks records: {total_greeks}")
    print(f"💧 Total liquidity records: {total_liquidity}")


                   

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

# Create and save liquidity data parquet
if liquidity_data:
    # Create DataFrame with specified columns
    liquidity_df = pd.DataFrame(liquidity_data, columns=['ticker_id', 'entry_date'])
    
    # Save to parquet file
    liquidity_output_file = "option_liquidity.parquet"
    liquidity_df.to_parquet(liquidity_output_file)
    print(f"\nSaved {len(liquidity_df)} liquidity records to {liquidity_output_file}")
    
    # Print summary statistics
    print("\nLiquidity Summary Statistics:")
    print(f"Number of unique tickers: {liquidity_df['ticker_id'].nunique()}")
    print(f"Date range: {liquidity_df['entry_date'].min()} to {liquidity_df['entry_date'].max()}")
else:
    print("No liquidity data was collected.")



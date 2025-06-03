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

# Initialize database connection
db = Database()
with db:
    tickers = db.get_tickers()
    for ticker in tickers:
        close_price = db.get_ticker_prices(ticker[0])
        for price in close_price: 
            exp_date = first_expiry_in_band(ticker[1], price[0])
            if exp_date is None:
                continue
            urls = {
                contract_query(ticker[1], price[1], "call", price[0], exp_date),
                contract_query(ticker[1], price[1], "put", price[0], exp_date)
            }
            for url in urls:
                response = requests.get(url)
                if response.status_code == 200:
                    option_contracts = response.json()
                    if 'results' in option_contracts and option_contracts['results']:
                        # Extract strike_price and ticker for each contract
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
                        for index, option in option_stats.iterrows():
                            P_obs = get_price_obs(option['ticker'], price[0])
                            if P_obs and 'results' in P_obs and P_obs['results']:
                                P_obs = P_obs['results'][0]['price']
                                cp = 1 if option['contract_type'] == 'call' else -1
                                #S, K, P_obs, cp, entry, exit, ticker
                                iv, delta, gamma, theta, vega = get_greeks(price[1], option['strike_price'], P_obs, cp, price[0], exp_date, ticker[1])
                                print("ticker: ", ticker[1])
                                print("S: ", price[1])
                                print("K: ", option['strike_price'])
                                print("P_obs: ", P_obs)
                                print("cp: ", cp)
                                print("entry: ", price[0])
                                print("exit: ", exp_date)
                                print("iv: ", iv)
                                print("delta: ", delta)
                                print("gamma: ", gamma)
                                print("theta: ", theta)
                                print("vega: ", vega)
                                print("--------------------------------")


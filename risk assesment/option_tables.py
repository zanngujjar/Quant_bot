#!/usr/bin/env python3
# fetch_aapl_options.py

import os
import sys
import json
from calendar import timegm
from datetime import date, timedelta, datetime, timezone
import requests
from dotenv import load_dotenv   # pip install python-dotenv

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# ── 1. read API key from .env ───────────────────────────────────────────────────
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    sys.exit("❌  POLYGON_API_KEY not found in .env")

# ── 2. Helper: first  20‑30 days out ---------------------------------

def first_expiry_in_band(symbol: str, entry: str, lo: int = 20, hi: int = 45):
    """Return *date* of earliest expiry whose DTE is between lo and hi days.
    Prioritizes expiries closest to lo (20 days) within the range.
    Only returns options that existed on the entry date."""
    start = datetime.fromisoformat(entry).date()
    lo_day = (start + timedelta(days=lo)).isoformat()
    hi_day = (start + timedelta(days=hi)).isoformat()
    

    url = "https://api.polygon.io/v3/reference/options/contracts"
    params = {
        "underlying_ticker": symbol,
        "as_of": entry,
        "expiration_date.gte": lo_day,
        "expiration_date.lte": hi_day,
        "expired": "false",  # Only get options that were active
        "limit": 1000,
        "apiKey": API_KEY,
    }
    
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        if 'results' not in data:
            return None
            
        # Filter expiries to only include those that existed on entry date
        valid_expiries = []
        for row in data.get("results", []):
            expiry_date = datetime.fromisoformat(row["expiration_date"]).date()
            # Only include if the option existed on entry date
            if expiry_date > start:  # Must be after entry date
                dte = (expiry_date - start).days
                if lo <= dte <= hi:  # Only include if within our DTE range
                    valid_expiries.append((expiry_date, dte))
        
        if not valid_expiries:
  
            # Try with a wider range but still respect entry date
            wider_hi = (start + timedelta(days=45)).isoformat()

            
            params["expiration_date.gte"] = start.isoformat()
            params["expiration_date.lte"] = wider_hi
            
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            
            valid_expiries = []
            for row in data.get("results", []):
                expiry_date = datetime.fromisoformat(row["expiration_date"]).date()
                if expiry_date > start:  # Must be after entry date
                    dte = (expiry_date - start).days
                    if lo <= dte <= hi:  # Only include if within our DTE range
                        valid_expiries.append((expiry_date, dte))
            
        
        # Sort by distance from lo (20 days)
        valid_expiries.sort(key=lambda x: abs(x[1] - lo))  # Sort by closest to lo (20 days)
        
        selected_date, selected_dte = valid_expiries[0]
        print(f"\nSelected expiry date: {selected_date} (DTE: {selected_dte} days)")
        print(f"Selected as closest to {lo} days within {lo}-{hi} day range")
            
        return selected_date
        
    except requests.RequestException as e:
        return None  # Silent return on request errors
    except Exception as e:
        return None  # Silent return on any other errors


def contract_query(symbol: str, close_px: float, contract_type: str, entry_day: str, exp_str: str) -> str:
    BASE = "https://api.polygon.io/v3/reference/options/contracts"
    lo = round(close_px * 0.85, 2)
    hi = round(close_px * 1.15, 2)
    return (
        f"{BASE}?underlying_ticker={symbol}"
        f"&as_of={entry_day}"
        f"&strike_price.gte={lo}&strike_price.lte={hi}"
        f"&expiration_date={exp_str}"
        f"&contract_type={contract_type}"
        f"&limit=1000"
        f"&apiKey={API_KEY}"
    )
def to_ns(dt_utc):
    return int(timegm(dt_utc.timetuple())*1e9 + dt_utc.microsecond*1e3)


def get_price_obs(symbol: str, Date: str) -> float:
    # API endpoint
    url = f"https://api.polygon.io/v3/trades/{symbol}"


    year, month, day = Date.split("-")
    # Your window: 19:55–20:00 UTC on 2025-06-03
    t0 = datetime(int(year), int(month), int(day), 19, 55, 0, tzinfo=timezone.utc)
    t1 = datetime(int(year), int(month), int(day), 20,  0, 0, tzinfo=timezone.utc)
    # Parameters
    params = {
        "timestamp.gte": to_ns(t0),   # ≥ 3:55:00 PM ET
        "timestamp.lt":  to_ns(t1),   #  < 4:00:00 PM ET
        "order": "desc",
        "limit": 1,
        "apiKey": API_KEY
    }

    # Make the API call
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
    
        data = response.json()
        return data

    except requests.RequestException as e:
        return None  # Silent return on request errors
    except Exception as e:
        return None  # Silent return on any other errors

def get_option_vol(option_ticker: str, date: str) -> dict:
    """
    Fetch daily aggregated OHLCV data for an options contract.
    
    Args:
        option_ticker (str): The ticker symbol of the options contract
        date (str): Date in YYYY-MM-DD format
    
    Returns:
        dict: Daily OHLCV data with structure:
        {
            'ticker': str,
            'results': [{
                'c': float,  # close price
                'h': float,  # high price
                'l': float,  # low price
                'o': float,  # open price
                'v': float,  # volume
                'n': int,    # number of transactions
                'vw': float  # volume weighted average price
            }]
        }
    """
    # API endpoint for daily aggregates
    url = f"https://api.polygon.io/v2/aggs/ticker/{option_ticker}/range/1/day/{date}/{date}"
    
    # Query parameters
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'limit': 1,
        'apiKey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] != 'OK' or not data.get('results'):
            return None
            
        return data
        
    except requests.RequestException as e:
        print(f"Error fetching data for {option_ticker}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {option_ticker}: {str(e)}")
        return None

def get_option_quotes(option_ticker: str, entry_day: str ) -> dict:
    """
    Fetch a single quote for an options contract at a specific nanosecond timestamp.
    
    Args:
        option_ticker (str): The ticker symbol of the options contract
        nanosecond (int): The nanosecond timestamp to query
    
    Returns:
        dict: Quote data with structure:
        {
            'results': [{
                'bid_price': float,    # bid price
                'bid_size': int,       # bid size in round lots
                'ask_price': float,    # ask price
                'ask_size': int,       # ask size in round lots
                'bid_exchange': int,   # bid exchange ID
                'ask_exchange': int,   # ask exchange ID
                'sequence_number': int,# sequence number of the quote
                'sip_timestamp': int   # nanosecond timestamp
            }],
            'status': str,
            'request_id': str
        }
    """
    # API endpoint for quotes
    url = f"https://api.polygon.io/v3/quotes/{option_ticker}"
    year, month, day = entry_day.split("-")
    # Your window: 19:55–20:00 UTC on 2025-06-03
    t0 = datetime(int(year), int(month), int(day), 19, 55, 0, tzinfo=timezone.utc)
    t1 = datetime(int(year), int(month), int(day), 20,  0, 0, tzinfo=timezone.utc)
    
    # Query parameters
    params = {
        "timestamp.lte": to_ns(t0),     # ≤ ns
        "timestamp.gte": to_ns(t1),     # ≥ ns
        "order": "desc",         # newest first
        "limit": 1,
        "apiKey": API_KEY,
        }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] != 'OK' or not data.get('results'):
            return None
            
        return data
        
    except requests.RequestException as e:
        print(f"Error fetching quote for {option_ticker}  {entry_day}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {option_ticker}: {str(e)}")
        return None

if __name__ == "__main__":
    P_obs = get_price_obs("O:AAPL250703C00210000", "2025-06-02")
    print(P_obs)
    option_quotes = get_option_quotes("O:A200619C00080000", "2020-05-06")
    print(option_quotes)
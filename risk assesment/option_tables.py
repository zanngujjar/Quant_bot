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


def get_price_obs(symbol: str, Date: str, session=None) -> float:
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
        if session:
            response = session.get(url, params=params, timeout=30)
        else:
            response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
    
        data = response.json()
        return data

    except requests.RequestException as e:
        return None  # Silent return on request errors
    except Exception as e:
        return None  # Silent return on any other errors

def get_option_vol(option_ticker: str, date: str, session=None) -> dict:
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
        if session:
            response = session.get(url, params=params, timeout=30)
        else:
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

def get_option_quotes(option_ticker: str, entry: str, session=None) -> dict:
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
    # Your window: 19:55–20:00 UTC on 2025-06-03
    # Query parameters
    year, month, day = entry.split("-")
    # Your window: 19:55–20:00 UTC on 2025-06-03
    t0 = datetime(int(year), int(month), int(day), 19, 55, 0, tzinfo=timezone.utc)
    t1 = datetime(int(year), int(month), int(day), 20,  0, 0, tzinfo=timezone.utc)
    params = {
        "timestamp.gte": to_ns(t0),
        "timestamp.lt": to_ns(t1),
        "order": "desc",         # newest first
        "limit": 1,
        "apiKey": API_KEY,
        }
    
    try:
        if session:
            response = session.get(url, params=params, timeout=30)
        else:
            response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] != 'OK' or not data.get('results'):
            return None
            
        return data
        
    except requests.RequestException as e:
        print(f"Error fetching quote for {option_ticker}  {entry}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {option_ticker}: {str(e)}")
        return None

def is_older_than_years(entry_date_str, years=3, reference_date=None):
    """
    Return True if entry_date_str (YYYY-MM-DD) is more than `years` years before reference_date.
    """
    entry = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
    ref   = reference_date or datetime.now().date()
    return (ref - entry).days > years * 365

def calibrate_spread_factor(quote, vol):
    """
    Given a recent bid/ask quote and the day's high/low, compute a factor to map high-low range to spread.
    quote = {'ask_price':…, 'bid_price':…}
    vol   = {'h': high, 'l': low}
    """
    spread   = quote['ask_price'] - quote['bid_price']
    mid      = (quote['ask_price'] + quote['bid_price']) / 2
    rel_range= (vol['h'] - vol['l']) / mid if mid else None
    return spread / rel_range if rel_range else None

def estimate_spread(vol, factor, mid_price):
    """
    Estimate bid-ask spread for old data given high/low and a calibration factor.
    vol        = {'h': high, 'l': low}
    factor     = calibration factor from calibrate_spread_factor
    mid_price  = (ask+bid)/2 from vol-stats context
    """
    rel_range  = (vol['h'] - vol['l']) / mid_price if mid_price else None
    return rel_range * factor if rel_range else None
if __name__ == "__main__":
    P_obs = get_price_obs("O:AAPL250703C00210000", "2025-06-02")
    print(P_obs)
    option_quotes = get_option_quotes("O:AAPL250703C00210000", "2025-06-02")
    print(option_quotes)
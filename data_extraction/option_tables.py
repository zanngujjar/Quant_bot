#!/usr/bin/env python3
# fetch_aapl_options.py

import os
import sys
import json
from datetime import date, timedelta, datetime
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
    
    print(f"\nSearching for options expiries:")
    print(f"Symbol: {symbol}")
    print(f"Entry date: {entry}")
    print(f"Looking between: {lo_day} and {hi_day} ({lo}-{hi} DTE)")

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
            print(f"No results found in response: {json.dumps(data, indent=2)}")
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
            print("No valid expiries found in the specified date range")
            # Try with a wider range but still respect entry date
            wider_hi = (start + timedelta(days=60)).isoformat()
            print(f"\nTrying wider range up to: {wider_hi}")
            
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
            
            if valid_expiries:
                print("\nFound expiries (date, DTE):")
                for exp_date, dte in sorted(valid_expiries, key=lambda x: x[1]):  # Sort by DTE
                    print(f"  {exp_date}: {dte} days")
            else:
                print("No valid expiries found even in wider range")
                return None
        
        # Sort by distance from lo (20 days)
        valid_expiries.sort(key=lambda x: abs(x[1] - lo))  # Sort by closest to lo (20 days)
        
        selected_date, selected_dte = valid_expiries[0]
        print(f"\nSelected expiry date: {selected_date} (DTE: {selected_dte} days)")
        print(f"Selected as closest to {lo} days within {lo}-{hi} day range")
            
        return selected_date
        
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None

# ── 3. Connect to DB and fetch first trade_window --------------------------

db = Database()
with db:
    tws = db.get_trade_windows()
    if not tws:
        sys.exit("❌ No trade windows found in database")

    tw = tws[0]
    entry_day = tw["start_date"]  # ISO string yyyy‑mm‑dd
    d_entry = datetime.fromisoformat(entry_day).date()

    # Map trade_type → long / short legs (0 ⇒ long B/short A , 1 ⇒ long A/short B)
    if tw["trade_type"] == 0:
        long_id, short_id = tw["ticker_id1"], tw["ticker_id2"]
    else:
        long_id, short_id = tw["ticker_id2"], tw["ticker_id1"]

    long_sym = db.get_ticker_symbol(long_id)
    short_sym = db.get_ticker_symbol(short_id)

    long_close = db.get_ticker_prices(long_id, entry_day, entry_day)[0][1]
    short_close = db.get_ticker_prices(short_id, entry_day, entry_day)[0][1]

    print(f"\nTrade window details:")
    print(f"Entry date: {entry_day}")
    print(f"Long position: {long_sym} at ${long_close}")
    print(f"Short position: {short_sym} at ${short_close}")

# ── 4. Determine target expiry ( 20‑30 days out) ---------------------
expiry_date = first_expiry_in_band(long_sym, entry_day)
if not expiry_date:
    sys.exit("❌ first friday 20-30 days out not found")
exp_str = expiry_date.isoformat()
print(f"\nTarget expiry: {exp_str} (Friday {expiry_date:%j})")

# ── 5. Build Polygon reference/contracts URL ------------------------------
BASE = "https://api.polygon.io/v3/reference/options/contracts"

def contract_query(symbol: str, close_px: float, contract_type: str) -> str:
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

urls = {
    "LONG CALLs": contract_query(long_sym, long_close, "call"),
    "SHORT PUTs": contract_query(short_sym, short_close, "put"),
}

# ── 6. Query Polygon and print compact summaries ---------------------------
for label, url in urls.items():
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("results", [])
        print(f"\n{label}: {len(rows)} contracts found")
        print(json.dumps(rows[:2], indent=2))  # preview first two rows
        if data.get("next_url"):
            print(" …more pages available → follow next_url for the rest")
    except requests.RequestException as exc:
        print(f"\n{label}: request failed → {exc}")
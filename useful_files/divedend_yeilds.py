import os
import requests
import datetime
import pandas as pd
import sys
from dotenv import load_dotenv
from tqdm import tqdm
# Add the parent directory to the Python path to import the database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

API_KEY = os.getenv("POLYGON_API_KEY")
BASE = "https://api.polygon.io/v3/reference/dividends"

def get_dividend_data(ticker: str, days_back: int = 5*365) -> pd.DataFrame:
    """
    Fetch dividend data for a given ticker from Polygon API
    
    Args:
        ticker: Stock ticker symbol
        days_back: Number of days to look back (default: 5 years)
        
    Returns:
        DataFrame containing dividend data
    """
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY environment variable not set")

    # Calculate date range
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=days_back)

    params = {
        "ticker": ticker,
        "ex_dividend_date.gte": start_date.isoformat(),
        "ex_dividend_date.lte": end_date.isoformat(),
        "limit": 1000,
        "apiKey": API_KEY
    }

    all_rows = []
    url = BASE
    
    try:
        while url:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                break
                
            all_rows.extend(results)
            url = data.get("next_url")
            params = None  # next_url already has the API key
            
        if not all_rows:
            return pd.DataFrame()
            
        # Create DataFrame with all available columns
        df = pd.DataFrame(all_rows)
        
        # Convert date columns to datetime
        date_columns = ['ex_dividend_date', 'declaration_date', 'record_date', 'pay_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Sort by ex-dividend date
        df = df.sort_values('ex_dividend_date', ascending=False)
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error for {ticker}: {e}")
        return pd.DataFrame()

def process_all_tickers() -> None:
    """Process all tickers and store their dividend information in the database"""
    try:
        with Database() as db:
            tickers = db.get_all_tickers()
            all_dividends = []
            
            # Create progress bar
            pbar = tqdm(tickers, desc="Processing tickers", unit="ticker")
            
            for ticker_id, symbol in pbar:
                pbar.set_description(f"Processing {symbol}")
                
                # Get dividend data
                df = get_dividend_data(symbol)
                
                if df.empty:
                    continue
                
                # Filter for CD and SC type dividends
                df = df[df['dividend_type'].isin(['CD', 'SC'])]
                
                if df.empty:
                    continue
                
                # Convert frequency to numeric value
                def convert_frequency(freq):
                    if pd.isna(freq):
                        return 4  # Default to quarterly if unknown
                    freq = str(freq).lower()
                    if 'annual' in freq or 'yearly' in freq:
                        return 1
                    elif 'semi-annual' in freq or 'semi annual' in freq:
                        return 2
                    elif 'quarterly' in freq:
                        return 4
                    elif 'monthly' in freq:
                        return 12
                    return 4  # Default to quarterly
                
                # Collect dividend data in tuples
                for _, row in df.iterrows():
                    ex_date = row['ex_dividend_date'].strftime('%Y-%m-%d')
                    amount = float(row['cash_amount'])
                    div_type = row.get('dividend_type', 'CD')
                    frequency = convert_frequency(row.get('frequency', 'quarterly'))
                    
                    all_dividends.append((symbol, ex_date, amount, div_type, frequency))
            
            # Batch upload all dividends
            if all_dividends:
                print(f"\nUploading {len(all_dividends)} dividend records...")
                db.add_dividends_batch(all_dividends)
                print("Upload complete!")
            else:
                print("No dividend records found to upload.")
                
    except Exception as e:
        print(f"Error processing tickers: {e}")

if __name__ == "__main__":
    process_all_tickers()

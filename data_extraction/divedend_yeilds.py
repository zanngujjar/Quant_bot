import os
import requests
import datetime
import pandas as pd
import sys
from dotenv import load_dotenv
# Add the parent directory to the Python path to import the database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

API_KEY  = os.getenv("POLYGON_API_KEY")
BASE = "https://api.polygon.io/v3/reference/dividends"

def safe_date_convert(date_value) -> str:
    """
    Safely convert a date value to string format, handling NaT values
    
    Args:
        date_value: Date value to convert
        
    Returns:
        String date in YYYY-MM-DD format or None if invalid
    """
    try:
        if pd.isna(date_value):
            return None
        return pd.to_datetime(date_value).strftime('%Y-%m-%d')
    except Exception:
        return None

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
            print(f"No dividend data found for {ticker}")
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

def analyze_dividend_history(ticker: str) -> None:
    """
    Analyze and display dividend history for a given ticker
    
    Args:
        ticker: Stock ticker symbol
    """
    try:
        # Get dividend data
        df = get_dividend_data(ticker)
        
        if df.empty:
            return
            
        print(f"\nDividend History for {ticker}:")
        print("-" * 80)
        
        # Display dividend history
        for _, row in df.iterrows():
            ex_date = row['ex_dividend_date'].strftime('%Y-%m-%d')
            pay_date = row['pay_date'].strftime('%Y-%m-%d') if pd.notna(row['pay_date']) else 'N/A'
            amount = row['cash_amount']
            div_type = row.get('dividend_type', 'N/A')
            frequency = row.get('frequency', 'N/A')
            
            print(f"Ex-Date: {ex_date} | Pay Date: {pay_date} | Amount: ${amount:.2f} | Type: {div_type} | Frequency: {frequency}")
        
        # Calculate statistics
        total_dividends = len(df)
        total_amount = df['cash_amount'].sum()
        avg_amount = df['cash_amount'].mean()
        
        print("\nSummary Statistics:")
        print(f"Total Dividends: {total_dividends}")
        print(f"Total Amount Paid: ${total_amount:.2f}")
        print(f"Average Dividend: ${avg_amount:.2f}")
        
        # Calculate annual dividend growth
        if len(df) >= 2:
            first_div = df.iloc[-1]['cash_amount']
            last_div = df.iloc[0]['cash_amount']
            years = (df.iloc[0]['ex_dividend_date'] - df.iloc[-1]['ex_dividend_date']).days / 365.25
            growth_rate = ((last_div / first_div) ** (1/years) - 1) * 100
            
            print(f"Annual Dividend Growth Rate: {growth_rate:.2f}%")
        
    except Exception as e:
        print(f"Error analyzing dividend history for {ticker}: {e}")

if __name__ == "__main__":
    # Example usage
    ticker = "AAPL"
    analyze_dividend_history(ticker)

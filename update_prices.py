import requests
import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta, date
from time import sleep
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from DB.database import Base, PriceData, Ticker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === CONFIG ===
API_KEY = os.getenv('POLYGON_API_KEY')
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
DB_PATH = os.path.join(os.path.dirname(__file__), 'DB', 'quant_trading.db')

def get_latest_date(session):
    """Get the latest date in the price_data table"""
    latest_date = session.query(func.max(PriceData.date)).scalar()
    if not latest_date:
        # If no data exists, get last 30 days
        return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    return latest_date.strftime("%Y-%m-%d")

def fetch_prices(ticker, start_date, request_count):
    """Fetch price data for a single ticker"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    url = BASE_URL.format(symbol=ticker, start=start_date, end=end_date)
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": API_KEY
    }
    
    all_records = []
    while url:
        # Check if we need to sleep due to rate limit
        if request_count[0] % 5 == 0 and request_count[0] > 0:
            print(f"[i] Rate limit reached. Sleeping for 1 minute...")
            sleep(60)  # Sleep for 1 minute
        
        response = requests.get(url, params=params)
        request_count[0] += 1
        data = response.json()

        if 'results' not in data:
            print(f"[!] Failed for {ticker}: {data.get('error', 'Unknown error')}")
            return []

        # Add current batch of results
        all_records.extend(data['results'])
        
        # Get next URL if available
        url = data.get('next_url')
        if url:
            url += f'&apiKey={API_KEY}'
            print(f"[i] Fetching next batch for {ticker}...")
            sleep(1)  # Small delay between paginated requests

    return all_records

def update_database(records, ticker_id, session):
    """Update the database with new price records"""
    new_records = 0
    for record in records:
        # Convert timestamp to date object
        timestamp = record['t'] / 1000  # Convert milliseconds to seconds
        date_obj = datetime.utcfromtimestamp(timestamp).date()
        
        # Check if record already exists
        existing = session.query(PriceData).filter_by(
            ticker_id=ticker_id,
            date=date_obj
        ).first()
        
        if not existing:
            price_data = PriceData(
                ticker_id=ticker_id,
                date=date_obj,
                close_price=record.get('c')
            )
            session.add(price_data)
            new_records += 1
    
    return new_records

def main():
    print(f"Database path: {DB_PATH}")
    print(f"Database exists: {os.path.exists(DB_PATH)}")
    
    # Create database engine
    engine = create_engine(f'sqlite:///{DB_PATH}')
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get all tickers
        tickers = session.query(Ticker).all()
        print(f"Found {len(tickers)} tickers in database")
        
        if not tickers:
            print("No tickers found in database")
            return
        
        # Get latest date in database
        start_date = get_latest_date(session)
        print(f"Fetching data from {start_date} to present")
        
        # Track statistics
        request_count = [0]
        total_new_records = 0
        
        for ticker in tickers:
            try:
                print(f"\nProcessing {ticker.symbol}...")
                
                # Fetch new records
                records = fetch_prices(ticker.symbol, start_date, request_count)
                if not records:
                    continue
                
                # Update database
                new_records = update_database(records, ticker.ticker_id, session)
                total_new_records += new_records
                
                print(f"Added {new_records} new records for {ticker.symbol}")
                
            except Exception as e:
                print(f"Error processing {ticker.symbol}: {str(e)}")
                continue
        
        # Commit all changes
        session.commit()
        
        print(f"\nUpdate complete:")
        print(f"Total new records added: {total_new_records}")
        print(f"Total API requests made: {request_count[0]}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main() 
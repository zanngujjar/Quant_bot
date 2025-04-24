from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database import Base, PriceData, Ticker
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get database path from environment variables
DB_PATH = os.getenv('DB_PATH', 'quant_trading.db')  # Default value if not set

def check_prices():
    """Check price data for ticker 'A'"""
    # Create database engine
    engine = create_engine(f'sqlite:///{DB_PATH}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get ticker 'A'
        ticker = session.query(Ticker).filter_by(symbol='A').first()
        if not ticker:
            print("Ticker 'A' not found in database")
            return
        
        # Get all prices for ticker 'A'
        prices = session.query(PriceData).filter_by(ticker_id=ticker.ticker_id).order_by(PriceData.date).all()
        
        if not prices:
            print("No price data found for ticker 'A'")
            return
        
        # Get oldest and newest prices
        oldest = prices[0]
        newest = prices[-1]
        
        print(f"\nPrice data for {ticker.symbol}:")
        print(f"Oldest record: {oldest.date} - Close: ${oldest.close_price:.2f}")
        print(f"Newest record: {newest.date} - Close: ${newest.close_price:.2f}")
        print(f"Total records: {len(prices)}")
        
        # Print all prices in chronological order
        print("\nAll price records:")
        for price in prices:
            print(f"{price.date}: ${price.close_price:.2f}")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    check_prices() 
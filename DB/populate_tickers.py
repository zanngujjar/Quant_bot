import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, Ticker, HighCorrelation
from tqdm import tqdm

def populate_tickers(csv_file='option_underlyings.csv', db_url='sqlite:///quant_trading.db'):
    # Create database engine
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Read CSV file
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            
            # Get total number of rows for progress bar
            total_rows = sum(1 for _ in open(csv_file)) - 1  # Subtract header row
            
            # Create ticker objects
            new_tickers = 0
            existing_tickers = 0
            
            for row in tqdm(reader, total=total_rows, desc="Processing tickers"):
                symbol = row[0].strip()  # Get symbol from first column
                if symbol:  # Only process if symbol is not empty
                    # Check if ticker already exists
                    existing_ticker = session.query(Ticker).filter_by(symbol=symbol).first()
                    if existing_ticker:
                        existing_tickers += 1
                    else:
                        ticker = Ticker(symbol=symbol)
                        session.add(ticker)
                        new_tickers += 1
            
            # Commit the transaction
            session.commit()
            print(f"Ticker population complete:")
            print(f"- Added {new_tickers} new tickers")
            print(f"- Skipped {existing_tickers} existing tickers")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        session.rollback()
    finally:
        session.close()

def populate_correlations(corr_file='high_corr_pairs.csv', db_url='sqlite:///quant_trading.db'):
    # Create database engine
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Cache ticker symbols and IDs
        ticker_cache = {}
        for ticker in session.query(Ticker).all():
            ticker_cache[ticker.symbol] = ticker.ticker_id
        
        # Read CSV file
        with open(corr_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            
            # Get total number of rows for progress bar
            total_rows = sum(1 for _ in open(corr_file)) - 1  # Subtract header row
            
            # Track statistics
            new_correlations = 0
            existing_correlations = 0
            missing_pairs = []
            
            for row in tqdm(reader, total=total_rows, desc="Processing correlations"):
                ticker_a = row[0].strip()
                ticker_b = row[1].strip()
                correlation = float(row[2])
                
                # Get ticker IDs from cache
                ticker_a_id = ticker_cache.get(ticker_a)
                ticker_b_id = ticker_cache.get(ticker_b)
                
                if ticker_a_id is not None and ticker_b_id is not None:
                    # Check if correlation already exists
                    existing_corr = session.query(HighCorrelation).filter_by(
                        ticker_a_id=ticker_a_id,
                        ticker_b_id=ticker_b_id,
                        window_size=20
                    ).first()
                    
                    if existing_corr:
                        # Update existing correlation if different
                        if existing_corr.correlation != correlation:
                            existing_corr.correlation = correlation
                            new_correlations += 1
                        else:
                            existing_correlations += 1
                    else:
                        # Create new correlation
                        corr = HighCorrelation(
                            ticker_a_id=ticker_a_id,
                            ticker_b_id=ticker_b_id,
                            correlation=correlation,
                            window_size=20
                        )
                        session.add(corr)
                        new_correlations += 1
                else:
                    missing_pairs.append(f"{ticker_a}/{ticker_b}")
            
            # Commit the transaction
            session.commit()
            print(f"\nCorrelation population complete:")
            print(f"- Added/Updated {new_correlations} correlations")
            print(f"- Skipped {existing_correlations} existing correlations")
            
            # Print warnings for missing pairs
            if missing_pairs:
                print("\nWarning: Could not find tickers for the following pairs:")
                for pair in missing_pairs:
                    print(f"- {pair}")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Then populate correlations
    populate_correlations() 
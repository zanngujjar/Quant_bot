import csv
import os
from tqdm import tqdm
from DB.database import Database

def import_cointegration_tests():
    """Import cointegration test results from CSV file into the database"""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'valid_pairs_20250426_165840.csv')
    
    try:
        # Connect to the database
        with Database() as db:
            # Count total rows for progress tracking
            with open(csv_path, 'r') as file:
                total_rows = sum(1 for _ in file) - 1  # Subtract header row
            
            # Open and read the CSV file
            with open(csv_path, 'r') as file:
                csv_reader = csv.DictReader(file)
                
                # Create progress bar
                progress_bar = tqdm(total=total_rows, desc="Importing cointegration tests", unit="pairs")
                processed = 0
                batch = []
                
                # Insert each cointegration test result
                for row in csv_reader:
                    ticker_a = row['ticker_a'].strip()
                    ticker_b = row['ticker_b'].strip()
                    p_value = float(row['p_value'])
                    beta = float(row['beta'])
                    test_date = row['test_date']
                    
                    if ticker_a and ticker_b:  # Skip empty tickers
                        # Get ticker IDs
                        ticker_id_a = db.get_ticker_id(ticker_a)
                        ticker_id_b = db.get_ticker_id(ticker_b)
                        
                        if ticker_id_a and ticker_id_b:  # Only add if both tickers exist
                            batch.append((ticker_id_a, ticker_id_b, p_value, beta, test_date))
                            processed += 1
                    
                    # Process batch when it reaches 1000
                    if len(batch) >= 1000:
                        db.cursor.executemany("""
                            INSERT OR REPLACE INTO cointegration_tests 
                            (ticker_id_1, ticker_id_2, p_value, beta, test_date) 
                            VALUES (?, ?, ?, ?, ?)
                        """, batch)
                        db.conn.commit()
                        batch = []
                    
                    # Update progress bar
                    progress_bar.update(1)
                
                # Process any remaining rows in the last batch
                if batch:
                    db.cursor.executemany("""
                        INSERT OR REPLACE INTO cointegration_tests 
                        (ticker_id_1, ticker_id_2, p_value, beta, test_date) 
                        VALUES (?, ?, ?, ?, ?)
                    """, batch)
                    db.conn.commit()
                
                progress_bar.close()
                print(f"\nSuccessfully imported {processed} cointegration test results into the database!")
                
    except Exception as e:
        print(f"Error importing cointegration tests: {e}")
        raise

if __name__ == "__main__":
    import_cointegration_tests()

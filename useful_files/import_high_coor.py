import csv
import os
from datetime import datetime
from tqdm import tqdm
from DB.database import Database

def import_high_correlations(batch_size=1000):
    """Import high correlation pairs from CSV file into the database using batch processing"""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'high_corr_pairs.csv')
    
    try:
        # Connect to the database
        with Database() as db:
            # Drop high correlations table
            print("Dropping existing high correlations table...")
            db.cursor.execute("DROP TABLE IF EXISTS high_correlations")
            db.conn.commit()
            
            # Recreate the high correlations table
            print("Recreating high correlations table...")
            db.cursor.execute("""
                CREATE TABLE IF NOT EXISTS high_correlations (
                    id             INTEGER   PRIMARY KEY AUTOINCREMENT,
                    ticker_id_1    INTEGER   NOT NULL,
                    ticker_id_2    INTEGER   NOT NULL,
                    correlation    REAL      NOT NULL,
                    date           DATE      NOT NULL,
                    CONSTRAINT     uix_highcorr_pair_date UNIQUE (ticker_id_1, ticker_id_2, date),
                    FOREIGN KEY (ticker_id_1) REFERENCES tickers(id),
                    FOREIGN KEY (ticker_id_2) REFERENCES tickers(id)
                )
            """)
            db.conn.commit()
            
            # Get current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Count total rows for progress tracking
            with open(csv_path, 'r') as file:
                total_rows = sum(1 for _ in file) - 1  # Subtract header row
            
            # Open and read the CSV file
            with open(csv_path, 'r') as file:
                csv_reader = csv.reader(file)
                next(csv_reader)  # Skip header row
                
                # Create progress bar
                progress_bar = tqdm(total=total_rows, desc="Importing correlation pairs", unit="pairs")
                processed = 0
                batch = []
                
                # Insert each correlation pair
                for row in csv_reader:
                    if row:  # Skip empty rows
                        ticker_a = row[0].strip()
                        ticker_b = row[1].strip()
                        correlation = float(row[2])
                        
                        if ticker_a and ticker_b:  # Skip empty tickers
                            # Get ticker IDs
                            ticker_id_a = db.get_ticker_id(ticker_a)
                            ticker_id_b = db.get_ticker_id(ticker_b)
                            
                            if ticker_id_a and ticker_id_b:  # Only add if both tickers exist
                                batch.append((ticker_id_a, ticker_id_b, correlation, current_date))
                                processed += 1
                    
                    # Process batch when it reaches the batch size
                    if len(batch) >= batch_size:
                        db.cursor.executemany("""
                            INSERT INTO high_correlations 
                            (ticker_id_1, ticker_id_2, correlation, date) 
                            VALUES (?, ?, ?, ?)
                        """, batch)
                        db.conn.commit()
                        batch = []
                    
                    # Update progress bar
                    progress_bar.update(1)
                
                # Process any remaining rows in the last batch
                if batch:
                    db.cursor.executemany("""
                        INSERT INTO high_correlations 
                        (ticker_id_1, ticker_id_2, correlation, date) 
                        VALUES (?, ?, ?, ?)
                    """, batch)
                    db.conn.commit()
                
                progress_bar.close()
                print(f"\nSuccessfully imported {processed} correlation pairs into the database!")
                
    except Exception as e:
        print(f"Error importing correlation pairs: {e}")
        raise

if __name__ == "__main__":
    import_high_correlations()

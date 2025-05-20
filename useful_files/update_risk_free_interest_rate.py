import pandas as pd
import sys
import os

# Add the parent directory to the Python path to import the database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

def update_risk_free_rates():
    """
    Update the database with risk-free rates from DGS1MO.csv
    """
    try:
        # Read the CSV file from the same directory
        df = pd.read_csv('DGS1MO.csv')
        
        # Convert date column to string format YYYY-MM-DD
        df['observation_date'] = pd.to_datetime(df['observation_date']).dt.strftime('%Y-%m-%d')
        
        # Remove rows with missing values
        df = df.dropna()
        
        # Connect to database
        with Database() as db:
            # Insert each row into the database
            for _, row in df.iterrows():
                db.set_risk_free_rate(row['observation_date'], float(row['DGS1MO']))
            
            print(f"Successfully updated {len(df)} risk-free rate records")
            
    except Exception as e:
        print(f"Error updating risk-free rates: {e}")
        raise

if __name__ == "__main__":
    update_risk_free_rates()

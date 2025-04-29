import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

def test_latest_cointegrated_pairs():
    """
    Test function to verify that all cointegrated pairs have the same latest test date.
    """
    print("Testing latest cointegrated pairs...")
    db = Database()
    
    try:
        with db:
            # Get the latest cointegrated pairs with their test dates
            pairs = db.get_latest_cointegrated_pairs()
            
            if not pairs:
                print("No cointegrated pairs found!")
                return
                
            print(f"\nFound {len(pairs)} cointegrated pairs")
            
            # Get the test date from the first pair
            expected_date = pairs[0][2]
            print(f"\nExpected test date for all pairs: {expected_date}")
            
            # Check if all pairs have the same test date
            all_same_date = True
            for ticker1, ticker2, test_date in pairs:
                if test_date != expected_date:
                    print(f"Warning: {ticker1}/{ticker2} has different test date: {test_date}")
                    all_same_date = False
            
            if all_same_date:
                print("\nAll pairs have the same test date!")
            else:
                print("\nNot all pairs have the same test date!")
            
    except Exception as e:
        print(f"Error testing latest cointegrated pairs: {e}")
        raise

if __name__ == "__main__":
    test_latest_cointegrated_pairs()

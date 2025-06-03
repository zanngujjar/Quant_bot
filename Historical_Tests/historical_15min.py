#!/usr/bin/env python3

import os
import sys
from typing import Optional, Tuple

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

def get_latest_cointegration_beta(ticker1: str, ticker2: str) -> Optional[float]:
    """Get the beta value from the latest cointegration test for a pair of tickers.
    
    Args:
        ticker1: First ticker symbol
        ticker2: Second ticker symbol
        
    Returns:
        Beta value from the latest cointegration test, or None if no test found
    """
    db = Database()
    
    with db:
        # Get ticker IDs
        ticker1_id = db.get_ticker_id(ticker1)
        ticker2_id = db.get_ticker_id(ticker2)
        
        if not ticker1_id or not ticker2_id:
            return None
            
        # Get latest cointegration test
        coint_test = db.get_latest_cointegration_test(ticker1_id, ticker2_id)
        
        if coint_test:
            return coint_test['beta']
        return None

def main():
    # Example usage
    beta = get_latest_cointegration_beta('A', 'SKX')
    if beta is not None:
        print(f"Latest cointegration beta: {beta:.4f}")
    else:
        print("No cointegration test found for this pair")

if __name__ == "__main__":
    main()

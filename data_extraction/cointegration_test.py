import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from datetime import datetime, timedelta

def get_pairs_log_prices(
    use_cointegration: bool = True,
    days: int = 30,
    max_p_value: float = 0.05,
    min_correlation: float = 0.8
) -> Dict[Tuple[str, str], List[Tuple[str, float, float]]]:
    """
    Get log prices for pairs from either cointegration or high correlation tables.
    
    Args:
        use_cointegration: If True, get pairs from cointegration table, else from high correlation
        days: Number of days of data to retrieve (default: 30)
        max_p_value: Maximum p-value threshold for cointegration pairs
        min_correlation: Minimum correlation threshold for high correlation pairs
        
    Returns:
        Dictionary mapping (ticker1, ticker2) to list of (date, log_price1, log_price2) tuples
    """
    db = Database()
    
    try:
        with db:
            # Get pairs based on source
            if use_cointegration:
                pairs = db.get_cointegrated_pairs(max_p_value=max_p_value)
            else:
                pairs = db.get_high_correlation_pairs(min_correlation=min_correlation)
            
            # Get latest log prices for all pairs
            log_prices = db.get_latest_log_prices_for_pairs(pairs, days=days)
            
            return log_prices
            
    except Exception as e:
        print(f"Error getting pairs log prices: {e}")
        raise

def run_engle_granger_test(log_prices: Dict[Tuple[str, str], List[Tuple[str, float, float]]]) -> Dict[Tuple[str, str], Dict]:
    """
    Run Engle-Granger cointegration test on pairs of log prices.
    
    Args:
        log_prices: Dictionary mapping (ticker1, ticker2) to list of (date, log_price1, log_price2) tuples
        
    Returns:
        Dictionary mapping (ticker1, ticker2) to test results containing:
        - p_value: Cointegration test p-value
        - beta: Beta coefficient from OLS regression
        - test_date: Date of the test
    """
    results = {}
    
    for pair, prices in log_prices.items():
        if len(prices) < 20:  # Need at least 20 data points for meaningful test
            continue
            
        # Convert to numpy arrays
        x = np.array([p[1] for p in prices])  # log_price1
        y = np.array([p[2] for p in prices])  # log_price2
        
        try:
            # Engle-Granger cointegration test
            _, p_value, _ = coint(x, y)
            
            # Fit OLS model to get beta
            model = sm.OLS(x, sm.add_constant(y)).fit()
            beta = model.params[1]
            
            results[pair] = {
                'p_value': p_value,
                'beta': beta,
                'test_date': datetime.now().date()
            }
            
        except Exception as e:
            print(f"Error testing pair {pair}: {str(e)}")
            continue
            
    return results

def analyze_pairs(
    use_cointegration: bool = True,
    days: int = 90,
    max_p_value: float = 0.05,
    min_correlation: float = 0.8,
    batch_size: int = 10
) -> Dict[Tuple[str, str], Dict]:
    """
    Get pairs and run Engle-Granger cointegration test on them in batches.
    
    Args:
        use_cointegration: If True, get pairs from cointegration table, else from high correlation
        days: Number of days of data to retrieve
        max_p_value: Maximum p-value threshold for cointegration pairs
        min_correlation: Minimum correlation threshold for high correlation pairs
        batch_size: Number of pairs to process in each batch
        
    Returns:
        Dictionary mapping (ticker1, ticker2) to test results containing:
        - p_value: Cointegration test p-value
        - beta: Beta coefficient from OLS regression
        - test_date: Date of the test
    """
    print("Connecting to database...")
    db = Database()
    all_results = {}
    
    try:
        with db:
            print("Fetching pairs...")
            # Get pairs based on source
            if use_cointegration:
                pairs = db.get_cointegrated_pairs(max_p_value=max_p_value)
            else:
                pairs = db.get_high_correlation_pairs(min_correlation=min_correlation)
            
            if not pairs:
                print("No pairs found!")
                return {}
                
            total_pairs = len(pairs)
            print(f"Found {total_pairs} pairs. Processing in batches of {batch_size}...")
            
            # Process pairs in batches
            for i in range(0, total_pairs, batch_size):
                batch = pairs[i:i + batch_size]
                print(f"\nProcessing batch {i//batch_size + 1}/{(total_pairs + batch_size - 1)//batch_size}...")
                
                # Get log prices for current batch
                batch_log_prices = {}
                for pair in batch:
                    try:
                        log_prices = db.get_latest_log_prices_for_pair(pair[0], pair[1], days=days)
                        if log_prices:  # Only add if we got data
                            batch_log_prices[pair] = log_prices
                    except Exception as e:
                        print(f"Error getting log prices for pair {pair}: {str(e)}")
                        continue
                
                if batch_log_prices:
                    # Run Engle-Granger test on current batch
                    batch_results = run_engle_granger_test(batch_log_prices)
                    all_results.update(batch_results)
                    
                    # Print progress for current batch
                    for pair, result in batch_results.items():
                        print(f"  {pair[0]}/{pair[1]}: p-value={result['p_value']:.6f}, beta={result['beta']:.4f}")
            
            print(f"\nAnalysis complete! Processed {len(all_results)} pairs successfully.")
            return all_results
            
    except Exception as e:
        print(f"Error in analyze_pairs: {e}")
        raise

def test_analyze_pairs():
    """Test function to run analyze_pairs and print results"""
    print("\nTesting analyze_pairs function...")
    
    # Test with both cointegration and correlation pairs
    for use_cointegration in [True, False]:
        pair_type = "cointegrated" if use_cointegration else "highly correlated"
        print(f"\nAnalyzing {pair_type} pairs...")
        
        try:
            results = analyze_pairs(
                use_cointegration=use_cointegration,
                days=90,
                max_p_value=0.05,
                min_correlation=0.8,
                batch_size=10  # Process 10 pairs at a time
            )
            
            if not results:
                print(f"No {pair_type} pairs found")
                continue
                
            print(f"\nFound {len(results)} {pair_type} pairs with valid results:")
            for pair, result in results.items():
                print(f"\nPair: {pair[0]}/{pair[1]}")
                print(f"  p-value: {result['p_value']:.6f}")
                print(f"  beta: {result['beta']:.4f}")
                print(f"  test date: {result['test_date']}")
                
        except Exception as e:
            print(f"Error analyzing {pair_type} pairs: {str(e)}")
            
    print("\nAnalysis complete!")

if __name__ == "__main__":
    test_analyze_pairs()

import numpy as np
from statsmodels.tsa.stattools import coint
from typing import List, Tuple, Dict, Optional
from datetime import datetime, timedelta
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
def get_pair_log_prices(db: Database, ticker_id_1: int, ticker_id_2: int, 
                       start_date: str, end_date: str,
                       prices_1: List[Tuple[int, str]], prices_2: List[Tuple[int, str]]) -> Tuple[List[float], List[float], List[str]]:
    """
    Get log prices for a pair of tickers within a date range.
    
    Args:
        db: Database instance
        ticker_id_1: First ticker ID
        ticker_id_2: Second ticker ID
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        prices_1: List of (price_id, date) tuples for first ticker
        prices_2: List of (price_id, date) tuples for second ticker
        
    Returns:
        Tuple of (log_prices_1, log_prices_2, dates) where dates are the common dates
    """
    # Filter prices to only include those within the window
    prices_1 = [(pid, date) for pid, date in prices_1 if start_date <= date <= end_date]
    prices_2 = [(pid, date) for pid, date in prices_2 if start_date <= date <= end_date]
    
    # Convert to dictionaries for easier lookup
    price_dict_1 = {date: price_id for price_id, date in prices_1}
    price_dict_2 = {date: price_id for price_id, date in prices_2}
    
    # Find common dates
    common_dates = sorted(set(price_dict_1.keys()) & set(price_dict_2.keys()))
    
    # Get log price IDs for all price IDs
    all_price_ids = [price_dict_1[date] for date in common_dates] + [price_dict_2[date] for date in common_dates]
    log_price_ids = db.get_log_price_ids_batch(all_price_ids)
    
    # Get log prices for both tickers
    log_prices_1 = []
    log_prices_2 = []
    valid_dates = []
    
    for date in common_dates:
        price_id_1 = price_dict_1[date]
        price_id_2 = price_dict_2[date]
        
        if price_id_1 in log_price_ids and price_id_2 in log_price_ids:
            log_prices_1.append(log_price_ids[price_id_1])
            log_prices_2.append(log_price_ids[price_id_2])
            valid_dates.append(date)
    
    return log_prices_1, log_prices_2, valid_dates

def run_engle_granger_test(log_prices_1: List[float], log_prices_2: List[float]) -> Tuple[float, float, float]:
    """
    Run Engle-Granger cointegration test on two price series.
    
    Args:
        log_prices_1: First price series
        log_prices_2: Second price series
        
    Returns:
        Tuple of (p_value, alpha, beta)
    """
    # Run cointegration test
    score, p_value, _ = coint(log_prices_1, log_prices_2)
    
    # Calculate beta (slope) using OLS
       # Full intercept+beta regression: y = α + β·x
    y = np.array(log_prices_1)
    x = np.array(log_prices_2)
    # build design matrix [1, x]
    X = np.column_stack((np.ones_like(x), x))   # shape (N,2)
    # solve for [α, β]
    alpha, beta = np.linalg.lstsq(X, y, rcond=None)[0]
    
    return p_value, alpha, beta

def analyze_single_pair(ticker_id_1: int = 1, ticker_id_2: int = 4521, window_size: int = 90) -> float:
    """
    Analyze a single pair of tickers using rolling windows.
    
    Args:
        ticker_id_1: First ticker ID (default: 1)
        ticker_id_2: Second ticker ID (default: 4521)
        window_size: Size of rolling window in days (default: 90)
        
    Returns:
        float: Time taken to process this pair in seconds
    """
    pair_start_time = time.time()
    
    with Database() as db:
        # Get all available price data for both tickers
        prices_1 = db.get_ticker_price_ids(ticker_id_1)
        prices_2 = db.get_ticker_price_ids(ticker_id_2)
        
        # Find common date range
        dates_1 = {date for _, date in prices_1}
        dates_2 = {date for _, date in prices_2}
        common_dates = sorted(dates_1 & dates_2)
        
        if not common_dates:
            print(f"No common dates found for tickers {ticker_id_1} and {ticker_id_2}")
            return time.time() - pair_start_time
            
        start_date = common_dates[0]
        end_date = common_dates[-1]
        
        print(f"Analyzing pair {ticker_id_1}-{ticker_id_2}")
        print(f"Date range: {start_date} to {end_date}")
        
        # Calculate number of windows
        total_days = len(common_dates)
        if total_days < window_size:
            print(f"Not enough data points. Need {window_size} days, but only have {total_days}")
            return time.time() - pair_start_time
            
        # Process each day after the initial window
        windows_processed = 0
        window_times = []
        skipped_windows = 0
        
        for i in range(window_size, total_days):
            window_start_time = time.time()
            
            # Get window dates (90 days before current date)
            window_start_idx = i - window_size
            window_end_idx = i
            window_dates = common_dates[window_start_idx:window_end_idx]
            test_date = common_dates[i]  # Current date is the test date
            
            start_date   = window_dates[0]
            end_date     = window_dates[-1] 

            # Get log prices for this window
            log_prices_1, log_prices_2, _ = get_pair_log_prices(
                db, ticker_id_1, ticker_id_2, 
                start_date, end_date,
                prices_1, prices_2
            )
            
            if len(log_prices_1) < window_size:
                print(f"Skipping window ending {test_date} - insufficient data")
                skipped_windows += 1
                continue
                
            # Run cointegration test
            p_value, alpha, beta = run_engle_granger_test(log_prices_1, log_prices_2)
            
            # Store results in database
            db.add_cointegration_test(
                ticker_id_1=ticker_id_1,
                ticker_id_2=ticker_id_2,
                p_value=p_value,
                alpha=alpha,
                beta=beta,
                test_date=test_date
            )
            
            window_time = time.time() - window_start_time
            window_times.append(window_time)
            windows_processed += 1
            
            if windows_processed % 10 == 0:
                avg_window_time = sum(window_times[-10:]) / min(10, len(window_times))
                print(f"Processed {windows_processed}/{total_days - window_size} windows | "
                      f"Last 10 windows avg: {avg_window_time:.3f}s | "
                      f"Current window: {window_time:.3f}s")

    pair_time = time.time() - pair_start_time
    
    # Display window timing statistics
    if window_times:
        avg_window_time = sum(window_times) / len(window_times)
        min_window_time = min(window_times)
        max_window_time = max(window_times)
        
        print(f"\nWindow timing statistics for pair {ticker_id_1}-{ticker_id_2}:")
        print(f"  Total windows processed: {windows_processed}")
        print(f"  Windows skipped: {skipped_windows}")
        print(f"  Average time per window: {avg_window_time:.3f} seconds")
        print(f"  Fastest window: {min_window_time:.3f} seconds")
        print(f"  Slowest window: {max_window_time:.3f} seconds")
        
        # Show distribution of window times
        slow_windows = sum(1 for t in window_times if t > avg_window_time * 1.5)
        print(f"  Slow windows (>1.5x avg): {slow_windows} ({slow_windows/len(window_times)*100:.1f}%)")
    
    print(f"Pair {ticker_id_1}-{ticker_id_2} completed in {pair_time:.2f} seconds")
    return pair_time

def main():
    """
    Main function to run the cointegration analysis with timing.
    """
    print("Starting cointegration analysis...")
    program_start_time = time.time()
    
    # List of pairs to analyze (currently just one pair, but can be extended)
    pairs_to_analyze = [
        (1, 4521),  # Default pair
        # Add more pairs here as needed: (ticker_id_1, ticker_id_2)
    ]
    
    pair_times = []
    successful_pairs = 0
    
    for i, (ticker_id_1, ticker_id_2) in enumerate(pairs_to_analyze, 1):
        print(f"\n--- Processing pair {i}/{len(pairs_to_analyze)}: {ticker_id_1}-{ticker_id_2} ---")
        try:
            pair_time = analyze_single_pair(ticker_id_1, ticker_id_2)
            pair_times.append(pair_time)
            successful_pairs += 1
        except Exception as e:
            print(f"Error processing pair {ticker_id_1}-{ticker_id_2}: {e}")
    
    # Calculate and display timing results
    total_time = time.time() - program_start_time
    
    print("\n" + "="*60)
    print("TIMING RESULTS")
    print("="*60)
    print(f"Total program execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"Pairs processed successfully: {successful_pairs}/{len(pairs_to_analyze)}")
    
    if pair_times:
        avg_time_per_pair = sum(pair_times) / len(pair_times)
        min_time = min(pair_times)
        max_time = max(pair_times)
        
        print(f"Average time per pair: {avg_time_per_pair:.2f} seconds")
        print(f"Fastest pair: {min_time:.2f} seconds")
        print(f"Slowest pair: {max_time:.2f} seconds")
        
        print("\nIndividual pair times:")
        for i, (pair, pair_time) in enumerate(zip(pairs_to_analyze, pair_times)):
            print(f"  Pair {pair[0]}-{pair[1]}: {pair_time:.2f} seconds")
    else:
        print("No pairs were processed successfully.")
    
    print("="*60)

if __name__ == "__main__":
    main()

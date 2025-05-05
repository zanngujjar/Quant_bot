import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from tqdm import tqdm
import multiprocessing as mp
from functools import partial

def most_frequent_one_unit_drop(epsilon_data: List[Dict]) -> float:
    """
    Count S → (S‑1) drops completed within 30 days of first slipping below S,
    WITHOUT resetting the timer if |z| briefly climbs above S in the meantime.
    """
    thresholds = [round(x / 10, 1) for x in range(20, 9, -1)]  # 2.0 … 1.0
    state = {S: {"above": False, "drop_start": None} for S in thresholds}
    drops = defaultdict(int)

    for row in epsilon_data:                                  # chronological
        d = row["date"]
        if isinstance(d, str):
            d = datetime.fromisoformat(d).date()
        abs_z = abs(row["z_score"])

        for S in thresholds:
            st = state[S]

            # ─── Enter / re‑enter ≥ S ──────────────────────────────
            if abs_z >= S and not st["above"] and st["drop_start"] is None:
                # Only mark 'above' if not already timing a drop
                st["above"] = True

            # ─── First slip below S (but not yet below S‑1) ─────────
            elif S - 1.0 <= abs_z < S and st["above"] and st["drop_start"] is None:
                st["drop_start"] = d                   # start 30‑day clock

            # ─── Final fall below S‑1  (complete drop) ─────────────
            elif abs_z < S - 1.0 and st["drop_start"]:
                if (d - st["drop_start"]).days <= 30:
                    drops[S] += 1                      # count the drop
                # reset for next cycle
                st["above"] = False
                st["drop_start"] = None

            # Any later spikes back ≥ S while drop_start is set are ignored:
            # they do NOT overwrite drop_start nor reset the timer.

    if not drops:
        return 7

    best = max(drops.items(), key=lambda kv: (kv[1], kv[0]))[0]
    return best

from datetime import datetime
from typing import List, Dict

def analyze_threshold_reversions(epsilon_data: List[Dict], threshold: float) -> Dict:
    """
    Analyze how often z-score reverts from a given threshold back toward it by 1.0 within 30 days.

    Args:
        epsilon_data: List of dicts (must be sorted by date)
        threshold: float (can be positive or negative)

    Returns:
        {
            'results': List of {
                'entry_date': date,
                'exit_date': date or None,
                'success': bool,
                'exit_price': float or None
            },
            'success_rate': float (0.0 to 1.0)
        }
    """
    in_trade = False
    trade_type = None  # 'upper' or 'lower'
    entry_date = None
    entry_index = None
    entries = []

    for i, row in enumerate(epsilon_data):
        z = row['z_score']
        d = row['date']
        if isinstance(d, str):
            d = datetime.fromisoformat(d).date()

        if not in_trade:
            if z >= threshold:
                # Enter upper trade
                in_trade = True
                trade_type = 'upper'
                entry_date = d
                entry_index = i
            elif z <= -threshold:
                # Enter lower trade
                in_trade = True
                trade_type = 'lower'
                entry_date = d
                entry_index = i
        else:
            if trade_type == 'upper':
                # Exit upper trade when z <= threshold - 1 (reversion toward 0)
                if z <= threshold - 1.0:
                    entries.append({
                        "entry_date": entry_date,
                        "exit_date": d,
                        "success": True,
                        "type": "upper"
                    })
                    in_trade = False
                    trade_type = None
                    entry_date = None
                    entry_index = None
                    continue
            elif trade_type == 'lower':
                # Exit lower trade when z >= -threshold + 1 (reversion toward 0)
                if z >= -threshold + 1.0:
                    entries.append({
                        "entry_date": entry_date,
                        "exit_date": d,
                        "success": True,
                        "type": "lower"
                    })
                    in_trade = False
                    trade_type = None
                    entry_date = None
                    entry_index = None
                    continue

            # Timeout logic (optional)
            if i - entry_index >= 30:
                entries.append({
                    "entry_date": entry_date,
                    "exit_date": d,
                    "success": False,
                    "type": trade_type
                })
                in_trade = False
                trade_type = None
                entry_date = None
                entry_index = None

    # Success rate
    total = len(entries)
    success_count = sum(1 for r in entries if r["success"])
    success_rate = success_count / total if total > 0 else 0.0

    return {
        "results": entries,
        "success_rate": success_rate
    }

def plot_zscore_trades(dates, z_scores, optimal_threshold, ticker_a, ticker_b):
    plt.figure(figsize=(12, 6))
    plt.plot(dates, z_scores, label='Z-Score', color='blue')
    plt.axhline(y=optimal_threshold, color='red', linestyle='--', label=f'Entry +{optimal_threshold:.1f}')
    plt.axhline(y=-optimal_threshold, color='red', linestyle='--', label=f'Entry -{optimal_threshold:.1f}')
    plt.axhline(y=optimal_threshold - 1, color='green', linestyle='--', label=f'Exit {optimal_threshold-1:.1f}')
    plt.axhline(y=-optimal_threshold + 1, color='green', linestyle='--', label=f'Exit -{optimal_threshold-1:.1f}')
    plt.title(f'Z-Score Analysis for {ticker_a}-{ticker_b} Pair')
    plt.xlabel('Date')
    plt.ylabel('Z-Score')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.show()

def process_pair(pair_data: Tuple) -> List[Tuple]:
    """Process a single pair and return trade windows"""
    # Create a new database connection for each process
    db = Database()
    db.connect()
    
    try:
        pair = (pair_data[0], pair_data[1])
        beta = pair_data[2]
        
        ticker_id_1 = db.get_ticker_id(pair[0])
        ticker_id_2 = db.get_ticker_id(pair[1])
        
        # Get epsilon prices for the pair
        epsilon_prices = db.get_epsilon_prices(ticker_id_1, ticker_id_2)
        
        if not epsilon_prices:
            return []
        
        # Convert to DataFrame for faster processing
        df = pd.DataFrame(epsilon_prices, columns=['date', 'epsilon', 'rolling_mean', 'rolling_std', 'z_score'])
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert to format needed for analysis
        epsilon_data = [
            {'date': row['date'].date(), 'z_score': row['z_score']}
            for _, row in df.iterrows()
        ]
        
        if not epsilon_data:
            return []
        
        optimal_threshold = most_frequent_one_unit_drop(epsilon_data)
        if optimal_threshold == 7:
            return []
            
        pos_results = analyze_threshold_reversions(epsilon_data, optimal_threshold)
        
        return [
            (
                ticker_id_1,
                ticker_id_2,
                optimal_threshold,
                1 if trade['success'] else 0,
                trade['entry_date'],
                trade['exit_date'],
                1 if trade['type'] == 'upper' else 0
            )
            for trade in pos_results['results']
        ]
    finally:
        db.close()

def chunk_data(data, chunk_size):
    """Split data into chunks for batch processing"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def main():
    # Initialize database connection for the main process
    db = Database()
    db.connect()

    try:
        # Get cointegrated pairs data
        cointegrated_data = db.get_latest_cointegrated_pairs()
        
        if cointegrated_data:
            # Create a pool of workers
            num_cores = mp.cpu_count()
            chunk_size = 1000  # Process 1000 pairs at a time to manage memory
            
            all_results = []
            total_chunks = (len(cointegrated_data) + chunk_size - 1) // chunk_size
            
            with tqdm(total=len(cointegrated_data), desc="Processing pairs", unit="pair") as pbar:
                for chunk in chunk_data(cointegrated_data, chunk_size):
                    # Process each chunk with a fresh pool
                    with mp.Pool(num_cores) as pool:
                        for result in pool.imap_unordered(process_pair, chunk):
                            all_results.extend(result)
                            pbar.update()
            
            # Batch insert all trade windows
            if all_results:
                # Process in chunks to avoid memory issues
                for chunk in chunk_data(all_results, 1000):
                    db.add_trade_windows_batch(chunk)
                print(f"Inserted {len(all_results)} trade windows into the database.")
        else:
            print("No cointegrated pairs found")
    
    finally:
        db.close()

if __name__ == "__main__":
    main()


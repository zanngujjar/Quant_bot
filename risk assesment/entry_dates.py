import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
from collections import defaultdict
from datetime import datetime
from typing import List, Dict
import pandas as pd
import numpy as np

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
    entries = []
    in_trade = False
    entry_date = None
    entry_index = None
    side = 1 if threshold > 0 else -1
    threshold = abs(threshold)

    for i, row in enumerate(epsilon_data):
        d = row['date']
        if isinstance(d, str):
            d = datetime.fromisoformat(d).date()
        abs_z = abs(row['z_score'])

        # ENTRY condition: crossed the threshold
        if not in_trade and abs_z >= threshold:
            in_trade = True
            entry_date = d
            entry_index = i

        # If in a trade, check for reversion
        elif in_trade:
            # Target is threshold - 1.0 (toward center)
            if abs_z <= threshold - 1.0:
                entries.append({
                    "entry_date": entry_date,
                    "exit_date": d,
                    "success": True,
                })
                in_trade = False
                entry_date = None
                entry_index = None
                continue

            # Timeout check (after 30 days)
            if i - entry_index >= 30:
                # Only record failure if not near end
                if i < len(epsilon_data) - 29:
                    entries.append({
                        "entry_date": entry_date,
                        "exit_date": d,
                        "success": False,
                    })
                in_trade = False
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

def main():
    # Initialize database connection
    db = Database()
    db.connect()

    try:
        # Get cointegrated pairs data
        cointegrated_data = db.get_latest_cointegrated_pairs()
        
        # Get only the first pair
        if cointegrated_data:
            first_pair = cointegrated_data[0]
            pair = (first_pair[0], first_pair[1])
            beta = first_pair[2]
            
            ticker_id_1 = db.get_ticker_id(pair[0])
            ticker_id_2 = db.get_ticker_id(pair[1])
            
            # Get epsilon prices for the pair
            epsilon_prices = db.get_epsilon_prices(ticker_id_1, ticker_id_2)
            
            if not epsilon_prices:
                print(f"No epsilon prices found for pair {pair[0]}-{pair[1]}")
                return
                
            # Convert to format needed for analysis - use all data points
            epsilon_data = []
            for date, epsilon, rolling_mean, rolling_std, z_score in epsilon_prices:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                epsilon_data.append({
                    'date': date_obj.date(),
                    'z_score': z_score
                })
            
            # Calculate optimal threshold and analyze reversions
            if epsilon_data:
                optimal_threshold = most_frequent_one_unit_drop(epsilon_data)
                print(f"\nResults for {pair[0]}-{pair[1]}:")
                print(f"Optimal threshold: {optimal_threshold}")
                print(f"Data points analyzed: {len(epsilon_data)}")
                
                # Analyze both positive and negative threshold crossings
                pos_results = analyze_threshold_reversions(epsilon_data, optimal_threshold)
                
                print("\nPositive threshold crossings:")
                print(f"Success rate: {pos_results['success_rate']:.2%}")
                print(f"Total trades: {len(pos_results['results'])}")
                print(f"Successful trades: {sum(1 for r in pos_results['results'] if r['success'])}")
                
                # Print detailed trade history
                print("\nDetailed Trade History:")
                print("Positive trades:")
                for trade in pos_results['results']:
                    status = "SUCCESS" if trade['success'] else "FAILURE"
                    print(f"Entry: {trade['entry_date']} -> Exit: {trade['exit_date']} ({status})")
                
                print("-" * 80)
        else:
            print("No cointegrated pairs found")
    
    finally:
        db.close()

if __name__ == "__main__":
    main()


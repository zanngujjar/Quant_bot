import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
from typing import List, Tuple, Dict
from datetime import datetime, timedelta

def analyze_cointegration_duration(max_p_value: float = 0.05) -> Dict[Tuple[str, str], List[Dict]]:
    """
    Analyze how long pairs stay cointegrated before breaking down.
    
    Args:
        max_p_value: Maximum p-value to consider a pair cointegrated (default: 0.05)
        
    Returns:
        Dictionary mapping (ticker1, ticker2) to list of cointegration periods,
        where each period contains:
        {
            'start_date': str,
            'end_date': str,
            'duration_days': int,
            'avg_p_value': float,
            'avg_beta': float
        }
    """
    with Database() as db:
        # Get all cointegration test results ordered by date
        results = db.get_cointegrated_pairs(max_p_value=1.0)  # Get all results to analyze
        
        # Group results by pair
        pair_results = {}
        for ticker1, ticker2, p_value, beta, test_date in results:
            pair = (ticker1, ticker2)
            if pair not in pair_results:
                pair_results[pair] = []
            pair_results[pair].append((test_date, p_value, beta))
        
        # Analyze each pair's cointegration periods
        pair_periods = {}
        for pair, tests in pair_results.items():
            # Sort tests by date
            tests.sort(key=lambda x: x[0])
            
            periods = []
            current_period = None
            
            for test_date, p_value, beta in tests:
                if p_value <= max_p_value:  # Cointegrated
                    if current_period is None:
                        # Start new period
                        current_period = {
                            'start_date': test_date,
                            'end_date': test_date,
                            'duration_days': 1,
                            'p_values': [p_value],
                            'betas': [beta]
                        }
                    else:
                        # Extend current period
                        current_period['end_date'] = test_date
                        current_period['duration_days'] += 1
                        current_period['p_values'].append(p_value)
                        current_period['betas'].append(beta)
                else:  # Not cointegrated
                    if current_period is not None:
                        # End current period
                        current_period['avg_p_value'] = sum(current_period['p_values']) / len(current_period['p_values'])
                        current_period['avg_beta'] = sum(current_period['betas']) / len(current_period['betas'])
                        del current_period['p_values']
                        del current_period['betas']
                        periods.append(current_period)
                        current_period = None
            
            # Handle case where last period extends to the end
            if current_period is not None:
                current_period['avg_p_value'] = sum(current_period['p_values']) / len(current_period['p_values'])
                current_period['avg_beta'] = sum(current_period['betas']) / len(current_period['betas'])
                del current_period['p_values']
                del current_period['betas']
                periods.append(current_period)
            
            if periods:
                pair_periods[pair] = periods
    
    return pair_periods

def print_cointegration_analysis(pair_periods: Dict[Tuple[str, str], List[Dict]]) -> None:
    """
    Print analysis of cointegration periods.
    
    Args:
        pair_periods: Dictionary of cointegration periods from analyze_cointegration_duration
    """
    print("\nCointegration Period Analysis")
    print("=" * 80)
    
    for (ticker1, ticker2), periods in pair_periods.items():
        print(f"\nPair: {ticker1}-{ticker2}")
        print("-" * 40)
        
        total_days = sum(p['duration_days'] for p in periods)
        avg_duration = total_days / len(periods) if periods else 0
        
        print(f"Number of cointegration periods: {len(periods)}")
        print(f"Total days cointegrated: {total_days}")
        print(f"Average period duration: {avg_duration:.1f} days")
        
        print("\nPeriods:")
        for i, period in enumerate(periods, 1):
            print(f"\nPeriod {i}:")
            print(f"  Start: {period['start_date']}")
            print(f"  End: {period['end_date']}")
            print(f"  Duration: {period['duration_days']} days")
            print(f"  Avg p-value: {period['avg_p_value']:.4f}")
            print(f"  Avg beta: {period['avg_beta']:.4f}")

if __name__ == "__main__":
    periods = analyze_cointegration_duration()
    print_cointegration_analysis(periods)

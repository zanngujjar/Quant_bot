import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Dict, List
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from datetime import datetime


from collections import defaultdict
from typing import List, Dict

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


class EpsilonPairSelector:
    def __init__(self, root):
        self.root = root
        self.root.title("Epsilon Pair Selector")
        
        # Initialize database connection
        self.db = Database()
        self.db.connect()
        
        # Get pairs and create mapping
        self.epsilon_pairs = self.db.get_epsilon_ticker_pairs()
        self.symbol_pairs = []
        self.ticker_a_to_b_mapping: Dict[str, List[str]] = {}
        self.symbol_to_id_mapping: Dict[str, int] = {}  # Add mapping to store symbol->id
        
        # Convert ID pairs to symbols and create mapping
        for pair in self.epsilon_pairs:
            symbol1 = self.db.get_ticker_symbol(pair[0])
            symbol2 = self.db.get_ticker_symbol(pair[1])
            self.symbol_pairs.append([symbol1, symbol2])
            
            # Store symbol to ID mapping
            self.symbol_to_id_mapping[symbol1] = pair[0]
            self.symbol_to_id_mapping[symbol2] = pair[1]
            
            # Create mapping of ticker A to all its ticker B pairs
            if symbol1 not in self.ticker_a_to_b_mapping:
                self.ticker_a_to_b_mapping[symbol1] = []
            self.ticker_a_to_b_mapping[symbol1].append(symbol2)
        
        # Create GUI elements
        self.create_widgets()
        
    def create_widgets(self):
        # Create and pack frames
        self.frame = ttk.Frame(self.root, padding="10")
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Ticker A Label and Dropdown
        ttk.Label(self.frame, text="Ticker A:").grid(row=0, column=0, padx=5, pady=5)
        self.ticker_a_var = tk.StringVar()
        self.ticker_a_dropdown = ttk.Combobox(
            self.frame, 
            textvariable=self.ticker_a_var,
            values=sorted(list(self.ticker_a_to_b_mapping.keys())),
            state="readonly"
        )
        self.ticker_a_dropdown.grid(row=0, column=1, padx=5, pady=5)
        
        # Ticker B Label and Dropdown
        ttk.Label(self.frame, text="Ticker B:").grid(row=1, column=0, padx=5, pady=5)
        self.ticker_b_var = tk.StringVar()
        self.ticker_b_dropdown = ttk.Combobox(
            self.frame,
            textvariable=self.ticker_b_var,
            state="readonly"
        )
        self.ticker_b_dropdown.grid(row=1, column=1, padx=5, pady=5)
        
        # Submit Button
        self.submit_button = ttk.Button(
            self.frame,
            text="Get Epsilon Prices",
            command=self.submit_pair
        )
        self.submit_button.grid(row=2, column=0, columnspan=2, pady=10)
        
        # Bind the update function to ticker A selection
        self.ticker_a_dropdown.bind('<<ComboboxSelected>>', self.update_ticker_b_dropdown)
    
    def update_ticker_b_dropdown(self, event=None):
        # Get selected ticker A
        selected_ticker_a = self.ticker_a_var.get()
        
        # Update ticker B dropdown with corresponding pairs
        if selected_ticker_a in self.ticker_a_to_b_mapping:
            self.ticker_b_dropdown['values'] = sorted(self.ticker_a_to_b_mapping[selected_ticker_a])
            self.ticker_b_var.set('')  # Clear current selection
    
    def submit_pair(self):
        ticker_a = self.ticker_a_var.get()
        ticker_b = self.ticker_b_var.get()
        
        if not ticker_a or not ticker_b:
            messagebox.showerror("Error", "Please select both tickers")
            return
            
        # Get ticker IDs from the mapping
        ticker_id_1 = self.symbol_to_id_mapping[ticker_a]
        ticker_id_2 = self.symbol_to_id_mapping[ticker_b]
        
        # Close any existing plots
        plt.close('all')
        
        # Query epsilon prices
        epsilon_prices = self.db.get_epsilon_prices(ticker_id_1, ticker_id_2)
        
        # Convert tuple data to list of dictionaries for analysis
        epsilon_data = []
        dates = []
        z_scores = []
        
        for date, epsilon, rolling_mean, rolling_std, z_score in epsilon_prices:
            # Convert date string to datetime object
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            dates.append(date_obj)
            z_scores.append(z_score)
            
            epsilon_data.append({
                'date': date_obj.date(),  # Convert to date for the analysis function
                'epsilon': epsilon,
                'rolling_mean': rolling_mean,
                'rolling_std': rolling_std,
                'z_score': z_score
            })
        
        # Calculate optimal threshold
        if epsilon_data:
            optimal_threshold = most_frequent_one_unit_drop(epsilon_data)
            
            # Create the plot
            plt.figure(figsize=(12, 6))
            
            # Plot z-score
            plt.plot(dates, z_scores, label='Z-Score', color='blue')
            
            # Plot threshold lines
            plt.axhline(y=optimal_threshold, color='red', linestyle='--', label=f'Entry +{optimal_threshold:.1f}')
            plt.axhline(y=-optimal_threshold, color='red', linestyle='--', label=f'Entry -{optimal_threshold:.1f}')
            
            # Plot exit threshold lines
            plt.axhline(y=optimal_threshold - 1, color='green', linestyle='--', label=f'Exit {optimal_threshold-1:.1f}')
            plt.axhline(y=-optimal_threshold + 1, color='green', linestyle='--', label=f'Exit -{optimal_threshold-1:.1f}')
            
            # Customize the plot
            plt.title(f'Z-Score Analysis for {ticker_a}-{ticker_b} Pair')
            plt.xlabel('Date')
            plt.ylabel('Z-Score')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Format x-axis dates
            plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
            
            # Show the plot
            plt.tight_layout()
            plt.show()
        
    def __del__(self):
        # Close database connection when object is destroyed
        self.db.close()

def main():
    root = tk.Tk()
    app = EpsilonPairSelector(root)
    root.mainloop()

if __name__ == "__main__":
    main()

import sys
import os
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

def export_cointegrated_pairs():
    with Database() as db:
        pairs = db.get_cointegrated_pairs(max_p_value=1)
        
        with open('cointegrated_pairs.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Symbol_1', 'Symbol_2', 'P_Value', 'Beta', 'Test_Date'])
            writer.writerows(pairs)
        
        print(f"Exported {len(pairs)} pairs to cointegrated_pairs.csv")

if __name__ == "__main__":
    export_cointegrated_pairs()
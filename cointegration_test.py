import sqlite3
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Get database path from environment variables
DB_PATH = os.getenv('DB_PATH', 'DB/quant_trading.db')

# Create a thread-local storage for database connections
thread_local = threading.local()

def get_db_connection():
    """Get a new database connection"""
    return sqlite3.connect(DB_PATH)

# Cache for tickers and their data
ticker_cache = {}
last_cache_update = None
CACHE_EXPIRY_HOURS = 24  # Cache expires after 24 hours

def update_log_prices():
    """Update log_prices table with log prices and rolling standard deviations"""
    print("Connecting to database...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # ⚡ Ensure indexes exist
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_id ON price_data(price_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_price_id ON log_prices(price_id);")
    conn.commit()

    # 🔍 Efficient row existence check
    print("Checking for new price records...")
    cursor.execute("""
        SELECT COUNT(*)
        FROM price_data pd
        LEFT JOIN log_prices lp ON lp.price_id = pd.price_id
        WHERE lp.price_id IS NULL
    """)
    new_count = cursor.fetchone()[0]

    if new_count == 0:
        print("No new price records to process. Exiting.")
        conn.close()
        return

    # 🧠 Pull only missing price records
    print("Querying new price data...")
    query = """
        SELECT pd.price_id, pd.ticker_id, pd.date, pd.close_price
        FROM price_data pd
        JOIN tickers t ON pd.ticker_id = t.ticker_id
        LEFT JOIN log_prices lp ON pd.price_id = lp.price_id
        WHERE t.is_active = 1
        AND lp.price_id IS NULL
        ORDER BY pd.ticker_id, pd.date
    """
    df = pd.read_sql_query(query, conn, parse_dates=['date'])
    print(f"Found {len(df):,} new price records to process")

    if not df.empty:
        print("Calculating log prices and rolling standard deviations...")
        df['log_price'] = np.log(df['close_price'])
        df['rolling_std'] = df.groupby('ticker_id')['log_price'].transform(
            lambda x: x.rolling(window=20, min_periods=1).std()
        )

        # Drop rows with NaN or invalid values
        df.dropna(subset=['log_price', 'rolling_std'], inplace=True)

        if df.empty:
            print("No valid rows after filtering. Exiting.")
            conn.close()
            return

        # Prepare data for fast insert
        insert_data = [
            (row['price_id'], row['log_price'], row['rolling_std'], datetime.now())
            for _, row in df.iterrows()
        ]

        print("Inserting log prices...")
        cursor.executemany("""
            INSERT OR IGNORE INTO log_prices 
            (price_id, log_price, rolling_std, created_at)
            VALUES (?, ?, ?, ?)
        """, insert_data)

        conn.commit()
        print(f"✅ Inserted {len(insert_data):,} new log price records.")

    conn.close()
    print("✅ Log price update complete.")


def update_ticker_cache():
    """Update the ticker cache with fresh data from the database"""
    global ticker_cache, last_cache_update
    
    conn = get_db_connection()
    query = """
        SELECT t.symbol, pd.date, pd.close_price
        FROM price_data pd
        JOIN tickers t ON pd.ticker_id = t.ticker_id
        WHERE t.is_active = 1
    """
    df = pd.read_sql_query(query, conn, parse_dates=['date'])
    
    # Filter out low-priced names (last 10 trading days all >= $5)
    def passes_price_filter(group):
        last_10 = group.sort_values('date').tail(10)
        return (last_10['close_price'] >= 5).all()
    
    filtered = df.groupby('symbol').filter(passes_price_filter)
    
    # Filter out thin data (at least 20 records)
    counts = filtered['symbol'].value_counts()
    valid = counts[counts >= 20].index
    filtered = filtered[filtered['symbol'].isin(valid)].copy()
    
    # Update cache
    ticker_cache = filtered
    last_cache_update = datetime.now()
    print(f"Ticker cache updated at {last_cache_update}")
    

def get_high_correlation_pairs():
    """Get pairs from high_correlations table"""
    conn = get_db_connection()
    query = """
        SELECT 
            t1.symbol as symbol_a,
            t2.symbol as symbol_b,
            hc.pair_id
        FROM high_correlations hc
        JOIN tickers t1 ON hc.ticker_a_id = t1.ticker_id
        JOIN tickers t2 ON hc.ticker_b_id = t2.ticker_id
    """
    return pd.read_sql_query(query, conn)

def run_cointegration_test():
    """Run cointegration test on high correlation pairs"""
    print("Starting cointegration test...")
    global ticker_cache, last_cache_update
    
    # Check if cache needs updating
    if last_cache_update is None or (datetime.now() - last_cache_update).total_seconds() > CACHE_EXPIRY_HOURS * 3600:
        print("Updating ticker cache...")
        update_ticker_cache()
    
    print(f"\n[{datetime.now()}] Running cointegration test...")
    
    # Get pairs from high_correlations table
    print("Fetching high correlation pairs...")
    pairs_df = get_high_correlation_pairs()
    total_pairs = len(pairs_df)
    print(f"Total pairs to analyze: {total_pairs:,}")
    
    # Process in batches
    batch_size = 1000  # Reduced batch size since we're working with fewer pairs
    total_batches = (total_pairs + batch_size - 1) // batch_size
    total_cointegrated = 0
    
    # Store results for bulk insert
    results = []
    
    def analyze_pair(row):
        symbol_a, symbol_b, pair_id = row['symbol_a'], row['symbol_b'], row['pair_id']
        
        # Get log prices directly from log_prices table
        query = """
            SELECT 
                pd.date,
                lp1.log_price as log_price_a,
                lp2.log_price as log_price_b
            FROM price_data pd
            JOIN log_prices lp1 ON pd.price_id = lp1.price_id
            JOIN tickers t1 ON pd.ticker_id = t1.ticker_id
            JOIN price_data pd2 ON pd.date = pd2.date
            JOIN log_prices lp2 ON pd2.price_id = lp2.price_id
            JOIN tickers t2 ON pd2.ticker_id = t2.ticker_id
            WHERE t1.symbol = ? AND t2.symbol = ?
            ORDER BY pd.date
        """
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn, params=(symbol_a, symbol_b), parse_dates=['date'])
        
        if len(df) < 20:
            return None
        
        x = df['log_price_a']
        y = df['log_price_b']
        
        # Engle–Granger cointegration test
        _, p_value, _ = coint(x, y)
        
        if p_value < 0.05:
            model = sm.OLS(x, sm.add_constant(y)).fit()
            beta = model.params.iloc[1]
            
            return {
                'pair_id': pair_id,
                'p_value': p_value,
                'beta': beta,
                'test_date': datetime.now().date(),
                'pair': f'{symbol_a}/{symbol_b}'
            }
        return None
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_pairs)
            current_batch = pairs_df.iloc[start_idx:end_idx]
            
            print(f"\nProcessing batch {batch_num + 1}/{total_batches}...")
            futures = {executor.submit(analyze_pair, row): idx for idx, row in current_batch.iterrows()}
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Analyzing pairs"):
                try:
                    result = future.result()
                    if result:
                        total_cointegrated += 1
                        results.append(result)
                        print(f"\nFound cointegrated pair {total_cointegrated}: {result['pair']}")
                        print(f"  p-value: {result['p_value']:.6f}, β: {result['beta']:.4f}")
                except Exception as e:
                    print(f"\nError processing pair: {str(e)}")
    
    # Bulk insert results
    if results:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Prepare the bulk insert statement
        insert_query = """
            INSERT INTO cointegration_tests (pair_id, p_value, beta, test_date)
            VALUES (?, ?, ?, ?)
        """
        
        # Prepare the data for bulk insert
        insert_data = [
            (r['pair_id'], r['p_value'], r['beta'], r['test_date'])
            for r in results
        ]
        
        # Execute bulk insert
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"\nBulk inserted {len(results)} cointegration test results")
    
    print(f"\nCointegration test complete. Found {total_cointegrated} cointegrated pairs.")
    return True

if __name__ == "__main__":
    print("Starting program...")
    
    try:
        # Update log prices if needed
        print("\nChecking for new price records...")
        update_log_prices()
        print("Log price check completed")
        
        # Always run cointegration test
        print("\nRunning cointegration test...")
        run_cointegration_test()
        print("Cointegration test completed")
        
        print("\nProgram completed successfully!")
    except Exception as e:
        print(f"\nError in main program: {str(e)}")
        raise 
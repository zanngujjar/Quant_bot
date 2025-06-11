import pandas as pd
from datetime import datetime

def read_filtered_data():
    """
    Read and display information about the filtered ticker dates parquet file.
    """
    print("Reading filtered ticker dates...")
    df = pd.read_parquet("filtered_ticker_dates.parquet")
    
    # Display basic information
    print("\n" + "="*60)
    print("DATASET INFORMATION:")
    print("="*60)
    print(f"Total rows: {len(df):,}")
    print(f"Unique ticker IDs: {df['ticker_id'].nunique():,}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    #print(f"Date range: {df['entry_date'].min()} to {df['entry_date'].max()}")
    
    # Display sample of the data
    print("\n" + "="*60)
    print("SAMPLE DATA (first 5 rows):")
    print("="*60)
    print(df.head())
    
    # Display memory usage
    print("\n" + "="*60)
    print("MEMORY USAGE:")
    print("="*60)
    print(f"Total memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Display ticker counts
    print("\n" + "="*60)
    print("TICKER COUNTS:")
    print("="*60)
    ticker_counts = df['ticker_id'].value_counts()
    print(f"Min rows per ticker: {ticker_counts.min():,}")
    print(f"Max rows per ticker: {ticker_counts.max():,}")
    print(f"Mean rows per ticker: {ticker_counts.mean():.2f}")

if __name__ == "__main__":
    read_filtered_data()

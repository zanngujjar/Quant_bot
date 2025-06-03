import pandas as pd

def read_log_prices(n_rows: int = 30) -> None:
    """
    Read and display the first n rows of the log_prices.parquet file.
    
    Args:
        n_rows: Number of rows to display (default: 5)
    """
    try:
        # Read the parquet file
        df = pd.read_parquet("log_prices.parquet")
        
        # Display basic information
        print("\nDataFrame Info:")
        print("-" * 50)
        print(f"Total rows: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}")
        
        # Display first n rows
        print(f"\nFirst {n_rows} rows:")
        print("-" * 50)
        print(df.head(n_rows))
        
    except FileNotFoundError:
        print("Error: log_prices.parquet file not found.")
    except Exception as e:
        print(f"Error reading parquet file: {e}")

if __name__ == "__main__":
    read_log_prices()

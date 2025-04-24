import schedule
import time
from datetime import datetime, date
import subprocess
import pytz
import os
from dotenv import load_dotenv
from cointegration_test import run_cointegration_test

# Load environment variables
load_dotenv()

# Get configuration from environment variables
UPDATE_TIME = os.getenv('UPDATE_TIME', '16:00')  # Default to 4 PM if not set

def run_update_prices():
    """Run the price update script"""
    print(f"\n[{datetime.now()}] Running price update...")
    try:
        subprocess.run(['python', 'update_prices.py'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running price update: {e}")
        return False
    return True

def run_correlation_test():
    """Run the correlation test"""
    if not os.path.exists('check_correlations.py'):
        print("Error: check_correlations.py not found")
        return False
        
    print(f"\n[{datetime.now()}] Running correlation test...")
    try:
        subprocess.run(['python', 'check_correlations.py'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running correlation test: {e}")
        return False
    return True

def run_granger_test():
    """Print Granger test message"""
    print(f"\n[{datetime.now()}] Granger test")
    return True

def daily_task():
    """Main task to run at 4 PM"""
    # Run price update
    if not run_update_prices():
        print("Price update failed, skipping further tests")
        return
    
    # Run cointegration test
    if not run_cointegration_test():
        print("Cointegration test failed")
    
    # Check if it's Friday for correlation test
    if datetime.now().weekday() == 4:  # 4 is Friday
        if not run_correlation_test():
            print("Correlation test failed")
    
    # Always run Granger test after price update
    run_granger_test()

def main():
    # Set up the schedule
    schedule.every().day.at(UPDATE_TIME).do(daily_task)
    
    print("Starting main scheduler...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            daily_task()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\nStopping scheduler...")

if __name__ == "__main__":
    main() 
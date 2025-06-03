#!/usr/bin/env python3
# new_API.py

import os
import sys
import requests
from dotenv import load_dotenv

# Load .env file from the parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

# Get API key from environment variables
API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    sys.exit("❌  POLYGON_API_KEY not found in .env")

# API endpoint
url = "https://api.polygon.io/v3/trades/O:AAPL250703C00210000"

# Parameters
params = {
    "timestamp.gte": "2025-06-03T19:55:00Z",   # ≥ 3:59:00 PM ET
    "timestamp.lt":  "2025-06-03T20:00:00Z",   #  < 4:00:00 PM ET
    "order": "desc",
    "limit": 1,
    "apiKey": API_KEY
}

# Make the API call
try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    
    data = response.json()
    print("API Response:")
    print(data)
    
except requests.RequestException as e:
    print(f"Error making API request: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

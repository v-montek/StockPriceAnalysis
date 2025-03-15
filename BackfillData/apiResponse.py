import requests
import time
from config import API_KEY

def fetch_stock_data(url):
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        print(f"API rate limit exceeded. Pausing for 60 seconds. URL:{url}")
        time.sleep(60)
        return fetch_stock_data(url)
    print(f"Fetched Response using URL:{url}")
    return response.json() if response.status_code == 200 else None

def fetch_ticker_info(symbol):
    """Fetch stock metadata from Polygon API."""
    url = 'https://api.polygon.io/v3/reference/tickers'
    params = {'ticker': symbol, 'market': 'stocks'}
    headers = {'Authorization': f'Bearer {API_KEY}'}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 429:
        print("API rate limit exceeded. Pausing for 60 seconds")
        time.sleep(60)
        return fetch_ticker_info(symbol)
    return response.json() if response.status_code == 200 else None
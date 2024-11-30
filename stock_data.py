import requests
import logging

API_KEY = "XJ0OFBX1HLQD6OYM"
BASE_URL = "https://www.alphavantage.co/query"

def fetch_stock_data(symbol):
    """Fetch real-time stock data using Alpha Vantage API."""
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",  # Interval for stock data (1-minute granularity)
        "apikey": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        # Check if the response contains stock data
        if "Time Series (1min)" in data:
            latest_time = list(data["Time Series (1min)"].keys())[0]
            latest_data = data["Time Series (1min)"][latest_time]
            stock_price = latest_data["1. open"]  # Extract the opening price for the minute
            return stock_price, latest_time
        else:
            logging.error("Error fetching stock data: %s", data.get("Error Message", "Unknown error"))
            return None, None
    except Exception as e:
        logging.error("Error fetching stock data: %s", str(e))
        return None, None


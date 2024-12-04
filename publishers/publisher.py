import pickle
import socket
import random
import time

# List of stock symbols and their random price ranges
STOCKS = {
    "AAPL": (150, 200),
    "GOOGL": (2700, 3000),
    "MSFT": (280, 330),
    "TSLA": (600, 900),
    "AMZN": (3100, 3500),
}

PUBLISHER_IP = "18.117.254.115"  # Replace with the Publisher's IP
PUBLISHER_PORT = 6000           # Publisher's port


def generate_stock_data():
    """
    Generate random stock data for predefined symbols.
    Returns a dictionary with stock symbol and price information.
    """
    stock_data = {}
    for symbol, (low, high) in STOCKS.items():
        price = round(random.uniform(low, high), 2)
        stock_data[symbol] = {
            "symbol": symbol,
            "price": price,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    return stock_data


def send_stock_data(client, stock_data):
    """
    Send stock data to the Publisher.
    """
    try:
        for symbol, data in stock_data.items():
            # Prepare the data packet
            packet = {"action": "publish", "topic": symbol, "content": data}
            print(f"Sending data to Publisher: {packet}")
            client.send(pickle.dumps(packet))
            time.sleep(1)  # Send each stock update with a delay
    except Exception as e:
        print(f"Error sending stock data: {e}")


def main():
    """
    Main function to connect to the Publisher and send stock data periodically.
    """
    while True:
        try:
            # Connect to the Publisher
            print(f"Connecting to Publisher at {PUBLISHER_IP}:{PUBLISHER_PORT}...")
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((PUBLISHER_IP, PUBLISHER_PORT))
            print("Connected to Publisher successfully!")

            # Generate and send stock data
            while True:
                stock_data = generate_stock_data()
                send_stock_data(client, stock_data)

        except Exception as e:
            print(f"Connection error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

        finally:
            client.close()
            print("Disconnected from Publisher. Retrying...")

if __name__ == "__main__":
    main()

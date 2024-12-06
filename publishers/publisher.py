import socket
import json
import time
import argparse
import random


class Publisher:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.stocks = {
            "AAPL": (150, 200),
            "GOOGL": (2700, 3000),
            "MSFT": (280, 330),
            "TSLA": (600, 900),
            "AMZN": (3100, 3500),
        }

    def generate_stock_data(self):
        """Generate random stock prices for predefined symbols."""
        stock_updates = {}
        for symbol, (low, high) in self.stocks.items():
            price = round(random.uniform(low, high), 2)
            stock_updates[symbol] = {
                "symbol": symbol,
                "price": price,
                "timestamp": time.ctime()
            }
        return stock_updates

    def publish(self, topic, message):
        """Publish a message to a topic."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                print(f"Connecting to broker at {self.broker_host}:{self.broker_port}...")
                s.connect((self.broker_host, self.broker_port))
                print(f"Connected. Publishing to topic '{topic}'...")
                data = {"type": "publish", "topic": topic.lower(), "message": message}
                s.send(json.dumps(data).encode('utf-8'))  # Send data as JSON string
                print(f"Published: {data}")
        except Exception as e:
            print(f"Error publishing message: {e}")

    def start_publishing(self, topic, interval=5):
        """Continuously publish stock data to the broker."""
        print(f"Starting automated stock data publishing to topic '{topic}'...")
        while True:
            stock_data = self.generate_stock_data()
            for symbol, data in stock_data.items():
                self.publish(topic, data)  # Publish each stock update as a message
            time.sleep(interval)  # Wait before generating the next update


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Publisher client.")
    parser.add_argument('--host', type=str, required=True, help="Broker host address (e.g., 'localhost').")
    parser.add_argument('--port', type=int, required=True, help="Broker port number (e.g., 2000).")
    parser.add_argument('--topic', type=str, default="stocks", help="Topic to publish to (default: 'stocks').")
    parser.add_argument('--interval', type=int, default=5, help="Interval (in seconds) between stock updates.")
    
    args = parser.parse_args()

    publisher = Publisher(args.host, args.port)
    publisher.start_publishing(args.topic, args.interval)

import socket
import json
import time
import argparse


class Publisher:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port

    def publish(self, topic, message):
        """Publish a message to a topic."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                print(f"Connecting to broker at {self.broker_host}:{self.broker_port}...")
                s.connect((self.broker_host, self.broker_port))
                print(f"Connected. Publishing to topic '{topic}'...")
                data = {"type": "publish", "topic": topic.lower(), "message": message}
                s.send(json.dumps(data).encode('utf-8'))  # Send data as JSON string
        except Exception as e:
            print(f"Error publishing message: {e}")


def generate_random_stock_data():
    """
    Generates random stock data for predefined symbols.
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


def send_stock_data_to_client(client_socket):
    """
    Sends randomly generated stock data to the connected client.
    """
    try:
        while True:
            # Generate stock data
            stock_data = generate_random_stock_data()
            print("pub: ", stock_data)
            for symbol, data in stock_data.items():
                # Create the data packet
                packet = {"action": "publish", "topic": symbol, "content": data}
                print(f"Sending data to client: {packet}")
                # Serialize and send data
                client_socket.send(pickle.dumps(packet))
                time.sleep(1)  # Send one update per second
    except Exception as e:
        print(f"Error sending data to client: {e}")
    finally:
        client_socket.close()
        print("Client disconnected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Publisher client.")
    parser.add_argument('--host', type=str, required=True, help="Broker host address (e.g., 'localhost').")
    parser.add_argument('--port', type=int, required=True, help="Broker port number (e.g., 2000).")
    parser.add_argument('--topic', type=str, default="stocks", help="Topic to publish to (default: 'stocks').")
    
    args = parser.parse_args()

    publisher = Publisher(args.host, args.port)
    message = f"Stock update at {time.ctime()}"
    publisher.publish(args.topic, message)
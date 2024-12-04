import socket
import pickle
import random
import time


# Predefined stock symbols with random price ranges
STOCKS = {
    "AAPL": (150, 200),
    "GOOGL": (2700, 3000),
    "MSFT": (280, 330),
    "TSLA": (600, 900),
    "AMZN": (3100, 3500),
}


def setup_server():
    """
    Sets up the Publisher server to listen for incoming connections.
    """
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("0.0.0.0", 6000))  # Bind to all interfaces on port 6000
        server.listen(5)  # Allow up to 5 simultaneous connections
        print("Publisher is running and waiting for connections...")
        return server
    except Exception as e:
        print(f"Error setting up server: {e}")
        exit()


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
    server = setup_server()
    try:
        while True:
            # Accept new client connections
            client_socket, addr = server.accept()
            print(f"Connection established with {addr}")
            # Send random stock data to the connected client
            send_stock_data_to_client(client_socket)
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        server.close()

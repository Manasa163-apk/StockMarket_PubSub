import socket
import pickle
import sys


def connect_to_publisher(publisher_ip, publisher_port):
    """
    Connects to the Publisher at the specified IP and port.
    """
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((publisher_ip, publisher_port))
        print(f"Connected to Publisher at {publisher_ip}:{publisher_port}")
        return client
    except Exception as e:
        print(f"Error connecting to Publisher: {e}")
        sys.exit(1)


def receive_data_from_publisher(client):
    """
    Receives and prints data from the Publisher.
    """
    try:
        while True:
            data = client.recv(4096)  # Receive up to 4096 bytes
            if not data:
                break  # Connection closed by the Publisher
            stock_data = pickle.loads(data)  # Deserialize the data
            print(f"Received Stock Data: {stock_data}")
    except Exception as e:
        print(f"Error receiving data: {e}")
    finally:
        client.close()
        print("Disconnected from Publisher.")


if __name__ == "__main__":
    # Ensure correct usage
    if len(sys.argv) != 2:
        print("Usage: python broker.py <PUBLISHER_IP>")
        sys.exit(1)

    publisher_ip = sys.argv[1]
    publisher_port = 6000  # Default port for the Publisher

    # Connect to the Publisher and receive data
    client_socket = connect_to_publisher(publisher_ip, publisher_port)
    receive_data_from_publisher(client_socket)


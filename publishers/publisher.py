import socket
import pickle


def setup_server():
    """
    Sets up a server socket to listen for incoming stock data.
    """
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("0.0.0.0", 6000))  # Bind to all available interfaces on port 6000
        server.listen(5)
        print("Publisher is running and waiting for connections...")
        return server
    except Exception as e:
        print(f"Error setting up server: {e}")
        exit()


def receive_stock_data(client_socket):
    """
    Receives stock data from the feeder and prints it to the terminal.
    """
    try:
        while True:
            data = client_socket.recv(4096)
            if not data:
                break
            stock_info = pickle.loads(data)
            print(f"Received Stock Data: {stock_info}")
    except Exception as e:
        print(f"Error receiving data: {e}")
    finally:
        client_socket.close()
        print("Client disconnected.")


if __name__ == "__main__":
    server = setup_server()
    try:
        while True:
            client_socket, addr = server.accept()
            print(f"Connection established with {addr}")
            receive_stock_data(client_socket)
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        server.close()


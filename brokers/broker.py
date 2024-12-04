import socket
import threading
import pickle


class Broker:
    def __init__(self, host="127.0.0.1", port=6000):
        self.host = host
        self.port = port
        self.topics = {}  # Topic-to-subscriber mapping
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        print(f"Broker running on {self.host}:{self.port}")

    def handle_client(self, conn, addr):
        print(f"Connected to {addr}")
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                message = pickle.loads(data)
                self.process_message(message, conn)
            except Exception as e:
                print(f"Error handling client {addr}: {e}")
                break
        conn.close()
        print(f"Disconnected from {addr}")

    def process_message(self, message, conn):
        action = message.get("action")
        topic = message.get("topic")
        if action == "subscribe":
            if topic not in self.topics:
                self.topics[topic] = []
            self.topics[topic].append(conn)
            print(f"Subscriber added to topic '{topic}'")
        elif action == "publish":
            if topic in self.topics:
                for subscriber in list(self.topics[topic]):  # Use list to avoid issues when removing items
                    try:
                        subscriber.send(pickle.dumps(message))
                    except Exception as e:
                        print(f"Failed to send to subscriber, removing: {e}")
                        self.topics[topic].remove(subscriber)  # Clean up disconnected subscribers
                print(f"Message published to topic '{topic}': {message['content']}")
            else:
                print(f"No subscribers for topic '{topic}'")

    def start(self):
        while True:
            conn, addr = self.server.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    broker = Broker()
    broker.start()

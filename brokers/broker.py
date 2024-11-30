import socket
import threading
import pickle


class Broker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.topics = {}  # {topic: [subscriber_socket1, subscriber_socket2, ...]}
        self.lock = threading.Lock()

    def handle_client(self, client_socket, client_address):
        print(f"New connection from {client_address}")
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break

                message = pickle.loads(data)
                action = message.get("action")
                if action == "subscribe":
                    self.subscribe(client_socket, message["topic"])
                elif action == "publish":
                    self.publish(message["topic"], message["content"])
        except:
            print(f"Connection with {client_address} closed.")
        finally:
            client_socket.close()

    def subscribe(self, client_socket, topic):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = []
            self.topics[topic].append(client_socket)
        print(f"Client subscribed to topic: {topic}")

    def publish(self, topic, content):
        with self.lock:
            if topic in self.topics:
                message = {"topic": topic, "content": content}
                data = pickle.dumps(message)
                for subscriber in self.topics[topic]:
                    try:
                        subscriber.send(data)
                    except:
                        print("Failed to send message to subscriber.")
            else:
                print(f"No subscribers for topic: {topic}")

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Broker started on {self.host}:{self.port}")

        while True:
            client_socket, client_address = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket, client_address), daemon=True).start()


if __name__ == "__main__":
    broker = Broker("127.0.0.1", 8000)
    broker.start()

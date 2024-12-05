import socket
import pickle


class Subscriber:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port

    def subscribe(self, topic):
        """Subscribe to a topic and print updates."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                print(f"Connecting to broker at {self.broker_host}:{self.broker_port}...")
                s.connect((self.broker_host, self.broker_port))
                print(f"Connected. Subscribing to topic '{topic}'...")
                data = {"action": "subscribe", "topic": topic.lower()}
                s.send(pickle.dumps(data))
                while True:
                    message_data = s.recv(1024)
                    if message_data:
                        message = pickle.loads(message_data)
                        print(f"Update on '{message['topic']}': {message['message']}")
        except Exception as e:
            print(f"Error subscribing to topic: {e}")


if __name__ == "__main__":
    subscriber = Subscriber("localhost", 2001)
    subscriber.subscribe("stocks")
    
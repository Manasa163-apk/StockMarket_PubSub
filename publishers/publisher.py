import socket
import pickle
import time


class Publisher:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port

    def publish(self, topic, message):
        """Publish a message to a topic."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.broker_host, self.broker_port))
                data = {"action": "publish", "topic": topic.lower(), "message": message}
                s.send(pickle.dumps(data))
        except Exception as e:
            print(f"Error publishing message: {e}")


if __name__ == "__main__":
    publisher = Publisher("localhost", 2000)
    while True:
        topic = "stocks"
        message = f"Stock update at {time.ctime()}"
        publisher.publish(topic, message)
        time.sleep(1)  # Adjust for real-time stock data frequency
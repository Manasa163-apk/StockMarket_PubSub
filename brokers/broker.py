import socket
import threading
import pickle
import sqlite3
import time
import random


class Broker:
    def __init__(self, host, port, peers):
        self.host = host
        self.port = port
        self.peers = [(peer.split(":")[0], int(peer.split(":")[1])) for peer in peers]
        self.topics = {}  # topic -> latest message
        self.subscribers = {}  # topic -> list of subscriber connections
        self.db_file = "broker.db"  # SQLite database file
        self.init_database()

    def init_database(self):
        """Initialize the database for storing topics and messages."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS topics (
                    topic TEXT PRIMARY KEY,
                    latest_message TEXT
                )
            """
            )
            conn.commit()
            conn.close()
            print("Database initialized successfully.")
        except Exception as e:
            print(f"Error initializing database: {e}")

    def load_from_database(self):
        """Load topics and messages from the database into memory."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT topic, latest_message FROM topics")
            for topic, message in cursor.fetchall():
                self.topics[topic.lower()] = message  # Normalize to lowercase
            conn.close()
            print("Topics loaded from database.")
        except Exception as e:
            print(f"Error loading from database: {e}")

    def start(self):
        """Start the broker server."""
        self.load_from_database()
        print(f"Starting broker at {self.host}:{self.port}")
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind((self.host, self.port))
            server.listen(5)
            print(f"Broker is listening on {self.host}:{self.port}")

            threading.Thread(target=self.gossip_protocol, daemon=True).start()

            while True:
                conn, addr = server.accept()
                print(f"Connection established with {addr}")
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print(f"Error starting the broker: {e}")

    def handle_client(self, conn, addr):
        """Handle client (publisher or subscriber) requests."""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                request = pickle.loads(data)
                action = request.get("action")
                if action == "publish":
                    self.handle_publish(request)
                elif action == "subscribe":
                    self.handle_subscribe(request, conn)
                elif action == "sync":
                    self.handle_sync(request)
                elif action == "add_peer":  # adding peers
                    self.handle_add_peer(request)
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def handle_publish(self, request):
        """Handle publishing a new topic/message."""
        topic = request.get("topic").lower()  # Normalize topic to lowercase
        message = request.get("message")

        if topic not in self.topics or self.topics[topic] != message:
            self.topics[topic] = message
            self.save_to_database(topic, message)
            print(f"Published topic '{topic}' with message '{message}'")
            self.notify_subscribers(topic, message)

    def save_to_database(self, topic, message):
        """Save the latest message for a topic to the database."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO topics (topic, latest_message)
                VALUES (?, ?)
                ON CONFLICT(topic) DO UPDATE SET latest_message=excluded.latest_message
            """,
                (topic, message),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error saving to database: {e}")

    def handle_subscribe(self, request, conn):
        """Handle subscriber's subscription request."""
        topic = request.get("topic").lower()

        if topic not in self.subscribers:
            self.subscribers[topic] = []

        self.subscribers[topic].append(conn)

        message = self.topics.get(topic)
        if message is None:
            conn.send(pickle.dumps({"topic": topic, "message": "No messages yet."}))
        else:
            conn.send(pickle.dumps({"topic": topic, "message": message}))

    def notify_subscribers(self, topic, message):
        """Notify all subscribers of a new message on a topic."""
        if topic not in self.subscribers or not self.subscribers[topic]:
            return
        for subscriber in self.subscribers[topic]:
            try:
                subscriber.send(pickle.dumps({"topic": topic, "message": message}))
            except Exception:
                self.subscribers[topic].remove(subscriber)

    def handle_sync(self, request):
        """Merge state received from another broker."""
        peer_topics = request.get("topics")
        for topic, message in peer_topics.items():
            topic = topic.lower()
            if topic not in self.topics or self.topics[topic] != message:
                self.topics[topic] = message
                self.save_to_database(topic, message)

    def gossip_protocol(self):
    # """Periodically gossip with random peers."""
        while True:
            if not self.peers:
               print("No peers available for gossip.")  # Debug log
               time.sleep(5)
               continue

            peer = random.choice(self.peers)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect(peer)
                    s.send(pickle.dumps({"action": "sync", "topics": self.topics}))
                    print(f"Gossiped with peer {peer}.")  # Debug log
            except Exception as e:
                print(f"Gossip failed with peer {peer}: {e}")  # Debug log
            time.sleep(5)
    
    def handle_add_peer(self, request):
        """Handle adding a new peer to the broker."""
        new_peer = (request.get("host"), request.get("port"))
        if new_peer not in self.peers:
            self.peers.append(new_peer)
            print(f"Added new peer: {new_peer}")
        else:
            print(f"Peer {new_peer} already exists.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Broker for Pub-Sub system")
    parser.add_argument("--host", type=str, required=True, help="Broker host address")
    parser.add_argument("--port", type=int, required=True, help="Broker port number")
    parser.add_argument(
        "--peers", type=str, nargs="*", default=[], help="List of peer brokers (host:port)"
    )
    args = parser.parse_args()

    broker = Broker(args.host, args.port, args.peers)
    broker.start()
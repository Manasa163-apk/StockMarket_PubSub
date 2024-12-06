import socket
import threading
import json
import sqlite3
import argparse
import time

HEARTBEAT_INTERVAL = 2  # Seconds between sending heartbeats
FAILURE_THRESHOLD = 5   # Seconds before detecting a failed peer


class Broker:
    def __init__(self, host, port, global_state_file):
        self.host = host
        self.port = port
        self.peers = self.load_peers(global_state_file)
        self.topics = {}  # topic -> latest message
        self.subscribers = {}  # topic -> list of subscriber connections
        self.isCoordinator = False  # Leader election flag
        self.coordinator = None  # Tracks the current coordinator
        self.db_file = "broker.db"  # SQLite database file
        self.init_database()

        self.last_heartbeat = {peer: time.time() for peer in self.peers}  # Heartbeat tracking
        self.running = True  # To control threads

        # Trigger leader election and heartbeat mechanisms upon deployment
        threading.Thread(target=self.initiate_election, daemon=True).start()
        threading.Thread(target=self.start_heartbeat_server, daemon=True).start()
        threading.Thread(target=self.start_heartbeat_client, daemon=True).start()
        threading.Thread(target=self.detect_failures, daemon=True).start()

    def load_peers(self, global_state_file):
        """Load peers from the provided CSV file."""
        peers = []
        try:
            with open(global_state_file, 'r') as file:
                lines = file.readlines()
                for line in lines[1:]:  # Skip the header
                    ip, port = line.strip().split(',')
                    if (ip, int(port)) != (self.host, self.port):  # Avoid adding itself
                        peers.append((ip, int(port)))
        except Exception as e:
            print(f"Error loading peers from {global_state_file}: {e}")
        return peers

    def init_database(self):
        """Initialize the database for storing topics and messages."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS topics (
                    topic TEXT PRIMARY KEY,
                    latest_message TEXT
                )
            """)
            conn.commit()
            conn.close()
            print("Database initialized.")
        except Exception as e:
            print(f"Database initialization failed: {e}")

    def initiate_election(self):
        """Initiate the leader election process."""
        print(f"Broker at {self.host}:{self.port} initiating election.")
        higher_priority_peers = [
            peer for peer in self.peers
            if (peer[0], peer[1]) > (self.host, self.port)
        ]

        responses = []

        def send_election(peer):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(peer)
                message = {"type": "election", "sender": (self.host, self.port)}
                s.send(json.dumps(message).encode('utf-8'))
                s.settimeout(5)
                response = s.recv(1024)
                responses.append(json.loads(response.decode('utf-8')))
                s.close()
            except Exception as e:
                print(f"Failed to communicate with peer {peer}: {e}")

        # Send election messages to higher-priority peers
        threads = []
        for peer in higher_priority_peers:
            thread = threading.Thread(target=send_election, args=(peer,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        if not responses:
            # No responses, elect self as coordinator
            self.announce_coordinator()
        else:
            print(f"Broker at {self.host}:{self.port} waiting for a coordinator.")

    def announce_coordinator(self):
        """Announce that this broker is the new coordinator."""
        print(f"Broker at {self.host}:{self.port} is the new coordinator.")
        self.isCoordinator = True
        self.coordinator = (self.host, self.port)

        # Notify all peers
        for peer in self.peers:
            self.send_message(peer, {
                "type": "coordinator",
                "sender": (self.host, self.port),
            })

    def start_heartbeat_server(self):
        """Listen for heartbeat messages from peers."""
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind(("0.0.0.0", self.port))
        print(f"Broker {self.host}:{self.port} listening for heartbeats on port {self.port}.")

        while self.running:
            try:
                data, addr = server.recvfrom(1024)
                message = data.decode("utf-8")
                if message == "heartbeat" and addr in self.peers:
                    print(f"Heartbeat received from {addr}")
                    self.last_heartbeat[addr] = time.time()
            except Exception as e:
                print(f"Error receiving heartbeat: {e}")

    def start_heartbeat_client(self):
        """Send periodic heartbeat messages to peers."""
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while self.running:
            for peer in self.peers:
                try:
                    client.sendto("heartbeat".encode("utf-8"), peer)
                    print(f"Sent heartbeat to {peer}")
                except Exception as e:
                    print(f"Error sending heartbeat to {peer}: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def detect_failures(self):
        """Detect failed peers based on missed heartbeats."""
        while self.running:
            current_time = time.time()
            for peer, last_time in self.last_heartbeat.items():
                if current_time - last_time > FAILURE_THRESHOLD:
                    print(f"Peer {peer} is considered failed. Triggering election.")
                    if peer == self.coordinator:
                        self.initiate_election()  # Trigger a new election if the leader fails
            time.sleep(1)

    def send_message(self, peer, message):
        """Send a message to a peer."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(peer)
            s.send(json.dumps(message).encode('utf-8'))
            s.close()
        except Exception as e:
            print(f"Failed to send message to {peer}: {e}")

    def start(self):
        """Start the broker server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        print(f"Broker running on {self.host}:{self.port}")
        try:
            while True:
                conn, addr = server.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print(f"{e}")
        finally:
            self.running = False
            server.close()

    def handle_client(self, conn, addr):
        """Handle incoming client connections."""
        print(f"Connected to {addr}")
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode('utf-8'))
                self.process_message(message, conn)
            except Exception as e:
                print(f"Error handling client {addr}: {e}")
                break
        conn.close()
        print(f"Disconnected from {addr}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Broker for publish-subscribe system with leader election.")
    parser.add_argument("--host", type=str, required=True, help="Host IP address of the broker.")
    parser.add_argument("--port", type=int, required=True, help="Port number of the broker.")
    args = parser.parse_args()

    GLOBAL_STATE_FILE = "globalState.csv"  # File containing peer information

    broker = Broker(args.host, args.port, GLOBAL_STATE_FILE)
    broker.start()

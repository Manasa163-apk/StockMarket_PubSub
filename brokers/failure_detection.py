import socket
import threading
import time
import sys

HEARTBEAT_INTERVAL = 2  # Seconds between sending heartbeats
FAILURE_THRESHOLD = 5   # Seconds before considering a broker as failed


class Broker:
    def __init__(self, broker_id, broker_port, peer_brokers):
        """
        Initializes the broker with an ID, listening port, and peer brokers.
        :param broker_id: Unique ID of this broker
        :param broker_port: Port this broker will use for communication
        :param peer_brokers: List of (IP, Port) tuples of peer brokers
        """
        self.broker_id = broker_id
        self.broker_port = broker_port
        self.peer_brokers = peer_brokers
        self.last_heartbeat = {(peer_ip, peer_port): time.time() for peer_ip, peer_port in peer_brokers}
        self.running = True

    def start_heartbeat_server(self):
        """
        Starts a server to receive heartbeat messages from peers.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind(("0.0.0.0", self.broker_port))
        print(f"Broker {self.broker_id} listening for heartbeats on port {self.broker_port}...")

        while self.running:
            try:
                data, addr = server.recvfrom(1024)
                message = data.decode("utf-8")
                if message == "heartbeat" and addr in self.peer_brokers:
                    print(f"Heartbeat received from {addr}")
                    self.last_heartbeat[addr] = time.time()
                elif addr not in self.peer_brokers:
                    print(f"Unexpected heartbeat received from {addr}")
            except Exception as e:
                print(f"Error receiving heartbeat: {e}")

    def start_heartbeat_client(self):
        """
        Periodically sends heartbeat messages to peer brokers.
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.bind(("0.0.0.0", self.broker_port))  # Bind to the broker's own port
        while self.running:
            for peer_ip, peer_port in self.peer_brokers:
                try:
                    client.sendto("heartbeat".encode("utf-8"), (peer_ip, peer_port))
                    print(f"Sent heartbeat to {peer_ip}:{peer_port}")
                except Exception as e:
                    print(f"Error sending heartbeat to {peer_ip}:{peer_port} - {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def detect_failures(self):
        """
        Monitors the last heartbeat times to detect failures.
        """
        while self.running:
            current_time = time.time()
            for peer, last_time in self.last_heartbeat.items():
                if current_time - last_time > FAILURE_THRESHOLD:
                    print(f"Broker {peer} is considered failed (last heartbeat at {last_time})")
            time.sleep(1)

    def run(self):
        """
        Starts the heartbeat server, client, and failure detection in separate threads.
        """
        threading.Thread(target=self.start_heartbeat_server, daemon=True).start()
        threading.Thread(target=self.start_heartbeat_client, daemon=True).start()
        threading.Thread(target=self.detect_failures, daemon=True).start()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python broker.py <BROKER_ID> <BROKER_PORT> [<PEER_IP>:<PEER_PORT> ...]")
        sys.exit(1)

    # Parse command-line arguments
    broker_id = int(sys.argv[1])
    broker_port = int(sys.argv[2])
    peer_brokers = []
    for peer in sys.argv[3:]:
        ip, port = peer.split(":")
        peer_brokers.append((ip, int(port)))

    # Create and run the broker
    broker = Broker(broker_id, broker_port, peer_brokers)
    broker.run()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        broker.running = False
        print("Broker shutting down...")



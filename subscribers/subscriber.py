import socket
import threading
import tkinter as tk
import pickle


def subscribe_to_topic():
    topic = topic_entry.get()
    if topic:
        try:
            data = {"action": "subscribe", "topic": topic}
            client.send(pickle.dumps(data))
            status_label.config(text=f"Subscribed to topic '{topic}'")
        except Exception as e:
            status_label.config(text=f"Error subscribing to topic: {e}")


def receive_messages():
    while True:
        try:
            data = client.recv(1024)
            if not data:
                break
            message = pickle.loads(data)
            print(f"Received message: {message}")  # Debugging
            display_area.insert(tk.END, f"[{message['topic']}] {message['content']}\n")
        except Exception as e:
            print(f"Error receiving message: {e}")
            break


def setup_client():
    global client
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", 6000))  # Use correct port (6000)
        threading.Thread(target=receive_messages, daemon=True).start()
    except Exception as e:
        print(f"Error connecting to broker: {e}")
        exit()


if __name__ == "__main__":
    setup_client()

    root = tk.Tk()
    root.title("Subscriber")

    tk.Label(root, text="Topic:").pack()
    topic_entry = tk.Entry(root)
    topic_entry.pack()

    tk.Button(root, text="Subscribe", command=subscribe_to_topic).pack()
    status_label = tk.Label(root, text="")
    status_label.pack()

    display_area = tk.Text(root, height=15, width=50)
    display_area.pack()

    root.mainloop()

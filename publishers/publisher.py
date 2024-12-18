import socket
import tkinter as tk
import pickle


def publish_message():
    topic = topic_entry.get()
    message = message_entry.get()
    if topic and message:
        try:
            data = {"action": "publish", "topic": topic, "content": message}
            client.send(pickle.dumps(data))
            status_label.config(text=f"Message published to topic '{topic}'")
        except Exception as e:
            status_label.config(text=f"Error publishing message: {e}")


def setup_client():
    global client
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", 6000))  # Use correct port (6000)
    except Exception as e:
        print(f"Error connecting to broker: {e}")
        exit()


if __name__ == "__main__":
    setup_client()

    root = tk.Tk()
    root.title("Publisher")

    tk.Label(root, text="Topic:").pack()
    topic_entry = tk.Entry(root)
    topic_entry.pack()

    tk.Label(root, text="Message:").pack()
    message_entry = tk.Entry(root)
    message_entry.pack()

    tk.Button(root, text="Publish", command=publish_message).pack()
    status_label = tk.Label(root, text="")
    status_label.pack()

    root.mainloop()

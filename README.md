Here is a detailed **README.md** file for your project:

---

# Publish-Subscribe System

This is a basic implementation of a **Publish-Subscribe System** using Python, where:
- **Broker** acts as an intermediary to manage topics and route messages between publishers and subscribers.
- **Publisher** sends messages to specific topics.
- **Subscriber** listens for messages from topics it subscribes to.

## Features
- Real-time messaging between publishers and subscribers.
- Flexible architecture supporting multiple publishers and subscribers.
- Easy-to-use interface with `tkinter`.

---

## Requirements

Ensure you have the following installed:
- Python 3.8 or higher
- `tkinter` (pre-installed with Python on most systems)
- Basic knowledge of sockets and threading.

---

## File Structure

```
.
├── broker/
│   ├── __init__.py          # Initializes the broker module
│   └── broker.py            # Broker implementation
├── publisher/
│   ├── __init__.py          # Initializes the publisher module
│   └── publisher.py         # Publisher client with GUI
├── subscriber/
│   ├── __init__.py          # Initializes the subscriber module
│   └── subscriber.py        # Subscriber client with GUI
|__stock_data.py
└── README.md          # Project documentation
```

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Run the Broker

Start the broker to manage connections and topics:
```bash
python broker.py
```

The broker will start on `127.0.0.1:5000` by default.

---

## Running Clients

### 1. Publisher

Launch the publisher to send messages:
```bash
python3 publisher.py
```

**Steps**:
1. Enter the topic name in the **"Topic"** field.
2. Enter the message in the **"Message"** field.
3. Click **Publish** to send the message to the broker.

### 2. Subscriber

Launch the subscriber to receive messages:
```bash
python3 subscriber.py
```

**Steps**:
1. Enter the topic name in the **"Topic"** field.
2. Click **Subscribe** to subscribe to the topic.
3. Messages from the subscribed topic will appear in the text area.

---

## Example Workflow

1. **Start the Broker**:
   ```bash
   python3 broker.py
   ```

2. **Run a Subscriber**:
   - Subscribe to a topic, e.g., `stocks`.

3. **Run a Publisher**:
   - Publish a message to the topic `stocks`.

4. **Message Flow**:
   - The subscriber receives the message for the topic `stocks`.

---

## Expected Output

### Subscriber Window
```
Topic: stocks
[stocks] Tesla shares up 5%
```

### Publisher Status
```
Message published to topic 'stocks'
```

### Broker Logs
```
New connection from ('127.0.0.1', 60000)
Client subscribed to topic: stocks
Message published to topic: stocks
```

---

## Troubleshooting

### 1. Ports Already in Use
If the port `8000` is busy, modify the broker code to use a different port:
```python
broker = Broker("127.0.0.1", <NEW_PORT>)
```
Update the `setup_client()` method in both `publisher.py` and `subscriber.py`:
```python
client.connect(("127.0.0.1", <NEW_PORT>))
```

### 2. Messages Not Received
Ensure:
- The broker is running.
- The publisher and subscriber are connected to the same broker.

---

## Enhancements

- **Logging**: Add logs to track connections and message flow.
- **Persistence**: Store messages for offline subscribers.
- **Advanced Topics**: Add support for topic wildcards and patterns.

---

## License
This project is licensed under the MIT License.

Enjoy using this real-time messaging system! 🎉

--- 

Copy this `README.md` file into your project directory for easy reference.
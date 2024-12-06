# POC

import socket
import json

globalState = []
isCoordinator = False

# Receive the --host and --port to be aware of current node info
host = "localhost"
port = 3000

with open('globalState.csv', 'r') as file:
    lines = file.readlines()
    for line in lines[1:]:
        ip, port = line.strip().split(',')
        globalState.append((ip, int(port)))

print(globalState)

def becomeCoordinator():
    isCoordinator = True

    for node in globalState:
        if node != (host, port):
            s.send(json.dumps({'type': 'coordinator', 'sender': (host, port)}).encode('utf-8'))

def receiveCoordinator():
    isCoordinator = False

def initiateElection():
    print("Node", host, port, " - initiates an election.")
    higher_nodes = [node for node in globalState if node[0] > host]
    responded = False

    for node in globalState:
        try:
            s.send(json.dumps({'type': 'election', 'sender': (host, port)}).encode('utf-8'))
            s.settimeout(5)
            response = s.recv(1024).decode('utf-8')
            data = json.loads(response)

            if data['type'] == 'response':
                responded = True

        except socket.timeout:
            print("Node", host, port, " - did not receive any responses")

    if not responded:
        becomeCoordinator()


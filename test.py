"""Quick and dirty test script to verify manager handles register + setup-dht."""

import socket
import json
import time

MANAGER_IP = "127.0.0.1"
MANAGER_PORT = 1500

def send_and_recv(sock, msg, addr):
    sock.sendto(json.dumps(msg).encode(), addr)
    data, _ = sock.recvfrom(4096)
    response = json.loads(data.decode())
    print(f"  Sent: {msg}")
    print(f"  Got:  {response}\n")
    return response

# simulate 3 peers, each with their own socket (like their m_sock)
peers = [
    {"name": "Alice", "ip": "127.0.0.1", "m_port": "1501", "p_port": "1502"},
    {"name": "Bob",   "ip": "127.0.0.1", "m_port": "1503", "p_port": "1504"},
    {"name": "Carol", "ip": "127.0.0.1", "m_port": "1505", "p_port": "1506"},
]

sockets = []
for p in peers:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((p["ip"], int(p["m_port"])))
    sockets.append(s)

manager_addr = (MANAGER_IP, MANAGER_PORT)

# --- Test 1: Register all three peers ---
print("=== Registering peers ===")
for i, p in enumerate(peers):
    send_and_recv(sockets[i], {
        "cmd": "register",
        "args": [p["name"], p["ip"], p["m_port"], p["p_port"]]
    }, manager_addr)

# --- Test 2: Duplicate registration should fail ---
print("=== Duplicate register (should FAIL) ===")
send_and_recv(sockets[0], {
    "cmd": "register",
    "args": ["Alice", "127.0.0.1", "1501", "1502"]
}, manager_addr)

# --- Test 3: setup-dht with too few peers (should fail) ---
print("=== setup-dht size=5 with only 3 peers (should FAIL) ===")
send_and_recv(sockets[0], {
    "cmd": "setup-dht",
    "args": ["Alice", "5", "1950"]  
}, manager_addr)

# --- Test 4: setup-dht with valid params ---
print("=== setup-dht size=3 (should SUCCEED) ===")
resp = send_and_recv(sockets[0], {
    "cmd": "setup-dht",
    "args": ["Alice", "3", "1950"]
}, manager_addr)

if resp.get("status") == "SUCCESS":
    print("Peers in DHT:")
    for p in resp.get("peers", []):
        print(f"  {p['name']} @ {p['ip']}:{p['p_port']}")

# --- Test 5: register during CONSTRUCTING should fail ---
print("\n=== register during CONSTRUCTING (should FAIL) ===")
send_and_recv(sockets[0], {
    "cmd": "register",
    "args": ["Dave", "127.0.0.1", "1507", "1508"]
}, manager_addr)

# --- Test 6: dht-complete from leader ---
print("=== dht-complete from leader (should SUCCEED) ===")
send_and_recv(sockets[0], {
    "cmd": "dht-complete",
    "args": ["Alice"]
}, manager_addr)

# cleanup
for s in sockets:
    s.close()

print("Done!")
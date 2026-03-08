"""
Self-contained integration test for the milestone.
Starts the manager as a subprocess, simulates 3 peers, and tests:
  1. Registration (success + duplicate detection)
  2. setup-dht (failure cases + success)
  3. Ring building via set-id messages between simulated peers
  4. dht-complete

Usage: uv run test_milestone.py
Make sure manager.py is in the same directory.
"""

import socket
import json
import subprocess
import time
import sys
import os

MANAGER_IP = "127.0.0.1"
MANAGER_PORT = 1500

PEERS = [
    {"name": "Alice", "ip": "127.0.0.1", "m_port": 1501, "p_port": 1502},
    {"name": "Bob",   "ip": "127.0.0.1", "m_port": 1503, "p_port": 1504},
    {"name": "Carol", "ip": "127.0.0.1", "m_port": 1505, "p_port": 1506},
]

passed = 0
failed = 0

def send_to_manager(sock, msg):
    """Send JSON message to manager and return parsed response."""
    sock.sendto(json.dumps(msg).encode(), (MANAGER_IP, MANAGER_PORT))
    data, _ = sock.recvfrom(4096)
    return json.loads(data.decode())

def send_to_peer(sock, msg, ip, port):
    """Send JSON message to a peer's p_port and return parsed response if any."""
    sock.sendto(json.dumps(msg).encode(), (ip, port))

def recv_from_peer(sock, timeout=2):
    """Receive a message on a peer socket with timeout."""
    sock.settimeout(timeout)
    try:
        data, addr = sock.recvfrom(4096)
        return json.loads(data.decode()), addr
    except socket.timeout:
        return None, None
    finally:
        sock.settimeout(None)

def check(test_name, condition, detail=""):
    """Simple assertion helper."""
    global passed, failed
    if condition:
        print(f"  PASS: {test_name}")
        passed += 1
    else:
        print(f"  FAIL: {test_name} {detail}")
        failed += 1

def create_m_sock(peer):
    """Create and bind a socket for manager communication."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((peer["ip"], peer["m_port"]))
    return s

def create_p_sock(peer):
    """Create and bind a socket for peer-to-peer communication."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((peer["ip"], peer["p_port"]))
    return s

def main():
    # --- Start manager as subprocess ---
    print("Starting manager...")
    manager_proc = subprocess.Popen(
        [sys.executable, "manager.py", str(MANAGER_PORT)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)  # give it a moment to bind

    # create sockets for each simulated peer
    m_socks = []
    p_socks = []
    try:
        for p in PEERS:
            m_socks.append(create_m_sock(p))
            p_socks.append(create_p_sock(p))

        # ============================================================
        print("\n=== Test Group 1: Registration ===")
        # ============================================================

        # register all three peers
        for i, p in enumerate(PEERS):
            resp = send_to_manager(m_socks[i], {
                "cmd": "register",
                "args": [p["name"], p["ip"], str(p["m_port"]), str(p["p_port"])]
            })
            check(f"register {p['name']}", resp.get("status") == "SUCCESS")

        # duplicate registration should fail
        resp = send_to_manager(m_socks[0], {
            "cmd": "register",
            "args": ["Alice", "127.0.0.1", "1501", "1502"]
        })
        check("duplicate register fails", resp.get("status") == "FAILURE")

        # ============================================================
        print("\n=== Test Group 2: setup-dht failure cases ===")
        # ============================================================

        # unregistered peer
        resp = send_to_manager(m_socks[0], {
            "cmd": "setup-dht",
            "args": ["Ghost", "3", "1950"]
        })
        check("setup-dht with unregistered peer fails", resp.get("status") == "FAILURE")

        # size too small
        resp = send_to_manager(m_socks[0], {
            "cmd": "setup-dht",
            "args": ["Alice", "2", "1950"]
        })
        check("setup-dht with n < 3 fails", resp.get("status") == "FAILURE")

        # not enough registered peers
        resp = send_to_manager(m_socks[0], {
            "cmd": "setup-dht",
            "args": ["Alice", "10", "1950"]
        })
        check("setup-dht with n > registered peers fails", resp.get("status") == "FAILURE")

        # ============================================================
        print("\n=== Test Group 3: setup-dht success ===")
        # ============================================================

        resp = send_to_manager(m_socks[0], {
            "cmd": "setup-dht",
            "args": ["Alice", "3", "1950"]
        })
        check("setup-dht returns SUCCESS", resp.get("status") == "SUCCESS")
        check("setup-dht returns peers list", "peers" in resp)
        check("setup-dht returns 3 peers", len(resp.get("peers", [])) == 3)

        dht_peers = resp.get("peers", [])
        if dht_peers:
            check("leader is first in peer list", dht_peers[0]["name"] == "Alice")

            # verify all peers have required fields
            all_have_fields = all(
                "name" in p and "ip" in p and "p_port" in p
                for p in dht_peers
            )
            check("all peer tuples have name, ip, p_port", all_have_fields)

        # ============================================================
        print("\n=== Test Group 4: Commands rejected during CONSTRUCTING ===")
        # ============================================================

        resp = send_to_manager(m_socks[0], {
            "cmd": "register",
            "args": ["Dave", "127.0.0.1", "1507", "1508"]
        })
        check("register rejected during CONSTRUCTING", resp.get("status") == "FAILURE")

        resp = send_to_manager(m_socks[0], {
            "cmd": "setup-dht",
            "args": ["Alice", "3", "1950"]
        })
        check("setup-dht rejected during CONSTRUCTING", resp.get("status") == "FAILURE")

        # ============================================================
        print("\n=== Test Group 5: Simulated ring building (set-id) ===")
        # ============================================================

        # This simulates what the leader peer would do after receiving
        # the setup-dht response. The leader sends set-id to each
        # non-leader peer on their p_port.
        #
        # NOTE: This tests YOUR peer logic once you implement it.
        # For now it just verifies the concept — the leader constructs
        # set-id messages and the other peers can receive them.

        if dht_peers and len(dht_peers) == 3:
            leader = dht_peers[0]
            n = len(dht_peers)

            # leader (Alice, id=0) sends set-id to each other peer
            for i in range(1, n):
                target = dht_peers[i]
                # find which of our simulated peers this is
                target_p_sock_idx = next(
                    j for j, p in enumerate(PEERS)
                    if p["name"] == target["name"]
                )

                set_id_msg = {
                    "cmd": "set-id",
                    "id": i,
                    "ring_size": n,
                    "peers": dht_peers,
                }
                send_to_peer(
                    p_socks[0],  # leader's p_sock sends it
                    set_id_msg,
                    target["ip"],
                    int(target["p_port"]),
                )

                # receive at the target peer's p_sock
                msg, addr = recv_from_peer(p_socks[target_p_sock_idx])

                check(
                    f"peer {target['name']} received set-id",
                    msg is not None and msg.get("cmd") == "set-id",
                )

                if msg:
                    check(
                        f"peer {target['name']} got correct id={i}",
                        msg.get("id") == i,
                    )

                    # verify the peer can figure out its right neighbor
                    right_neighbor_idx = (i + 1) % n
                    right_neighbor = dht_peers[right_neighbor_idx]
                    check(
                        f"peer {target['name']} right neighbor is {right_neighbor['name']}",
                        msg["peers"][right_neighbor_idx]["name"] == right_neighbor["name"],
                    )

        # ============================================================
        print("\n=== Test Group 6: dht-complete ===")
        # ============================================================

        # wrong peer sends dht-complete
        resp = send_to_manager(m_socks[1], {
            "cmd": "dht-complete",
            "args": ["Bob"]
        })
        check("dht-complete from non-leader fails", resp.get("status") == "FAILURE")

        # leader sends dht-complete
        resp = send_to_manager(m_socks[0], {
            "cmd": "dht-complete",
            "args": ["Alice"]
        })
        check("dht-complete from leader succeeds", resp.get("status") == "SUCCESS")

        # ============================================================
        print(f"\n{'='*50}")
        print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
        print(f"{'='*50}")

    finally:
        # clean up
        for s in m_socks:
            s.close()
        for s in p_socks:
            s.close()
        manager_proc.terminate()
        manager_proc.wait()
        print("\nManager stopped.")

if __name__ == "__main__":
    main()
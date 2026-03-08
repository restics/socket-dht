
import socket
from log import logger
import json
import select, sys

manager_ip = sys.argv[1]
manager_port = int(sys.argv[2])
m_port = int(sys.argv[3])
p_port = int(sys.argv[4])
name = ""

table_data = {}
ring_id = -1 # default if not part of ring
right_neighbor = None

m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
m_sock.bind(("127.0.0.1", m_port))

p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
p_sock.bind(("127.0.0.1", p_port))

active = True
logger.info("Creating manager socket at %s", m_port)
logger.info("Creating peer socket at %s", p_port)

while (active):
    readable, _, _ = select.select([sys.stdin, m_sock, p_sock], [], [], 0.5)

    for source in readable:
        if source == sys.stdin:
            raw_cmd = sys.stdin.readline().strip()
            cmd, *args = raw_cmd.split(" ")
            # TODO: determine destination based on command
            m_sock.sendto(json.dumps({'cmd' : cmd, 'args' : args}).encode(), (manager_ip, manager_port))
            logger.info('sent request \"%s\" to %s', raw_cmd, m_sock)

        elif source == m_sock:
            data, addr = m_sock.recvfrom(4096)
            response = json.loads(data.decode())
            cmd = response['cmd']

            logger.info('command: %s, manager response: %s', cmd,  response['status'])
            if response['status'] == "FAILURE":
                continue
            match(cmd):
                case 'setup-dht':
                    name = response['name']
                    peers = response['peers']
                    ring_id = 0 # you are the leader
                    for i, peer in enumerate(peers):
                        if peer['p_port'] == str(p_port): 
                            continue
                        req = {'cmd': 'set-id', 'id': i, 'size' : len(peers), 'peers' : peers}
                        p_sock.sendto(json.dumps(req).encode(), (peer['ip'], int(peer['p_port'])))

                        addr = f"{peer['ip']}:{peer['p_port']}"
                        logger.info('sent %s to %s', req, addr)

                    right_neighbor = peers[1]
                    logger.info('setup-dht complete, signaling manager')
                    m_sock.sendto(json.dumps({'cmd' : 'dht-complete', 'args' : [name]}).encode(), (manager_ip, manager_port))

        elif source == p_sock:
            data, addr = p_sock.recvfrom(4096)
            response = json.loads(data.decode())

            cmd = response['cmd']
            match (cmd):
                case 'set-id':
                   logger.info('received %s', response)
                   ring_id = response['id']
                   right_neighbor = response['peers'][(ring_id + 1) % response['size']]

            

    

    
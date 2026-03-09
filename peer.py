"""Peer: controls a peer, which can represent any number of roles in the dht, such as leader or member"""


import socket
from log import logger
import json
import select, sys
import csv
import time

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

def next_prime(n):
    num = n + 1 # given will always be even
    while not is_prime(num):
        num += 1
    return num

def is_prime(n):
    for j in range(2,int(n**0.5)):
        if n % j == 0:
            return False
    return True

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
                    year = response['year']
                    ring_id = 0 # you are the leader
                    size = len(peers)

                    for i, peer in enumerate(peers):
                        if peer['p_port'] == str(p_port): 
                            continue
                        req = {'cmd': 'set-id', 'id': i, 'size' : size, 'peers' : peers}
                        p_sock.sendto(json.dumps(req).encode(), (peer['ip'], int(peer['p_port'])))

                        addr = f"{peer['ip']}:{peer['p_port']}"
                        logger.info('sent %s to %s', req, addr)

                    right_neighbor = peers[1]
                    logger.info('ring complete, populating dht...')

                    with open(f'details-{year}.csv', 'r') as csvfile:
                        # df = pandas.read_csv(f'details-{year}.csv')

                        nodecounter = [] * size
                        datareader = csv.DictReader(csvfile)
                        rows = list(datareader)
                        s = next_prime(2*len(rows))
                        for row in rows:
   
                            pos = int(row['EVENT_ID']) % s
                            dest_id = pos % size
                            nodecounter[pos] += 1
                            if dest_id == ring_id:
                                if pos in table_data:
                                    logger.info('hash collision? pos : %s, dest_id %s', pos, dest_id)
                                table_data[pos] = row
                                logger.info('hash valid for leader, storing...')
                            else:
                                logger.info('directing hash (pos: %s,dest_id %s) to the right neighbor...', pos, dest_id)
                                req = {'cmd': 'store', 'pos': pos, 'dest_id' : dest_id, 'data' : row}
                                p_sock.sendto(json.dumps(req).encode(), (right_neighbor['ip'], int(right_neighbor['p_port'])))
                                time.sleep(0.001) # prevent buffer overflow
                        for i, n in enumerate(nodecounter):
                            logger.info('%s entries stored at %s', i, n)
                    logger.info('dht construction complete, signaling manager!')
                    m_sock.sendto(json.dumps({'cmd' : 'dht-complete', 'args' : [name]}).encode(), (manager_ip, manager_port))

        elif source == p_sock:
            data, addr = p_sock.recvfrom(4096)
            response = json.loads(data.decode())

            cmd = response['cmd']
            logger.info('command received: %s from peer %s', cmd, addr)
            match (cmd):
                case 'set-id':
                   logger.info('received %s', response)
                   ring_id = response['id']
                   right_neighbor = response['peers'][(ring_id + 1) % response['size']]
                case 'store':     
                    dest_id = response['dest_id']
                    pos = response['pos']
                    row = response['data']
                    
                    if dest_id == ring_id:
                        if pos in table_data:
                            logger.info('hash collision? pos : %s, dest_id %s, tuple: %s', pos, dest_id, row)
                        table_data[pos] = row
                        logger.info('hash valid for this peer, storing...')
                    else:
                        logger.info('directing hash (pos: %s,dest_id %s) to the right neighbor...', pos, dest_id)
                        req = {'cmd': 'store', 'pos': pos, 'dest_id' : dest_id, 'data' : row}
                        p_sock.sendto(json.dumps(req).encode(), (right_neighbor['ip'], int(right_neighbor['p_port'])))


    
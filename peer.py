"""Peer: controls a peer, which can represent any number of roles in the dht, such as leader or member"""


import socket
from log import logger
import json
import select, sys
import csv
import time
import random

manager_ip = sys.argv[1]
manager_port = int(sys.argv[2])
m_port = int(sys.argv[3])
p_port = int(sys.argv[4])
name = ""
my_ip = '0.0.0.0'

s = 1
dhtpeers = [] # im not even sure if we are supposed to do this but there's no other way of implementing query-dht to match spec
table_data = {}
ring_id = -1 # default if not part of ring
right_neighbor = None

m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
m_sock.bind(("0.0.0.0", m_port))

p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
p_sock.bind(("0.0.0.0", p_port))
p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)

active = True
logger.info("Creating manager socket at %s", m_port)
logger.info("Creating peer socket at %s", p_port)

def next_prime(n):
    num = n + 1 # given will always be even
    while not is_prime(num):
        num += 1
    return num

def is_prime(n):
    for j in range(2,int(n**0.5) + 1):
        if n % j == 0:
            return False
    return True

def find_event(sender_name, event_id, id_seq, sender_ip, sender_p_port):
    """
    Spec for find-event command:
    
    :param peer_name: a str name of the peer to return the results to
    :param event_id: a str id of the id to look for
    :param id-seq: a list of ids already searched
    :param status: a status code of the search. 0 = query not found, 1 = found
    
    """
    event_id = int(event_id)
    pos = event_id % s
    dest_id = pos % len(dhtpeers)

    if dest_id == ring_id:
        for record in table_data.get(pos, []):
            if int(record['EVENT_ID']) == event_id:
                logger.info("query found!")
                req = {'cmd': 'find-event-return', 'event_id' : event_id, 'event' : record, 'status' : 1}
                p_sock.sendto(json.dumps(req).encode(), (sender_ip, int(sender_p_port)))
                return
    i = [idd for idd in range(len(dhtpeers)) if idd not in id_seq] # ids are assigned contiguously from 0
    
    if len(i) == 0: # query not found (we searched all dht peers)
        logger.info("query not found! returning 0 to original sender.")
        req = {'cmd': 'find-event-return', 'event_id' : event_id, 'event' : {}, 'status' : 0}
        p_sock.sendto(json.dumps(req).encode(), (sender_ip, int(sender_p_port)))
        return

    if ring_id not in id_seq:
        id_seq.append(ring_id)
    next_peer_id = random.sample(i,1)[0]

    logger.info("query not in table! hot potatoing to peer %s with id_seq %s", dhtpeers[next_peer_id]['name'], id_seq)
    req = {'cmd': 'find-event', 'sender_name': sender_name, 'event_id': event_id, 'id_seq' : id_seq,'sender_ip': sender_ip, 'sender_p_port': sender_p_port }
    p_sock.sendto(json.dumps(req).encode(), (dhtpeers[next_peer_id]['ip'], int(dhtpeers[next_peer_id]['p_port'])))
        
        
while (active):
    
    readable, _, _ = select.select([sys.stdin, m_sock, p_sock], [], [], 0.5)

    for source in readable:
        if source == sys.stdin:
            raw_cmd = sys.stdin.readline().strip()
            cmd, *args = raw_cmd.split(" ")
    
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
                case 'register':
                    my_ip = response['ip']
                    name = response['name']
                case 'setup-dht':
                    name = response['name']
                    peers = response['peers']
                    year = response['year']
                    ring_id = 0 # you are the leader
                    size = len(peers)
                    dhtpeers = peers
                    with open(f'details-{year}.csv', 'r') as csvfile:

                        nodecounter = [0 for _ in range(size)] 
                        datareader = csv.DictReader(csvfile)
                        rows = list(datareader)
                    s = next_prime(2*len(rows))
                    
                    for i, peer in enumerate(peers):
                        if peer['p_port'] == str(p_port): 
                            continue
                        req = {'cmd': 'set-id', 'id': i, 'size' : size, 'peers' : peers, 's' : s}
                        p_sock.sendto(json.dumps(req).encode(), (peer['ip'], int(peer['p_port'])))

                        addr = f"{peer['ip']}:{peer['p_port']}"
                        logger.info('sent %s to %s', req, addr)

                    right_neighbor = peers[1]
                    logger.info('ring complete, populating dht...')


                    
                    for row in rows:

                        pos = int(row['EVENT_ID']) % s
                        dest_id = pos % size
                        nodecounter[dest_id] += 1
                        if dest_id == ring_id:
                            if pos in table_data:
                                existing = table_data[pos]
                                logger.info('collision at pos %s: existing EVENT_IDS=%s, new EVENT_ID=%s', pos, [event['EVENT_ID'] for event in existing], row['EVENT_ID'])
                            else:
                                table_data[pos] = []
                            table_data[pos].append(row)
                            logger.info('hash valid for leader, storing...')
                        else:
                            logger.info('directing hash (pos: %s,dest_id %s) to the right neighbor...', pos, dest_id)
                            req = {'cmd': 'store', 'pos': pos, 'dest_id' : dest_id, 'data' : row}
                            p_sock.sendto(json.dumps(req).encode(), (right_neighbor['ip'], int(right_neighbor['p_port'])))
                            time.sleep(0.002) # prevent buffer overflow

                    logger.info('%s total entries', len(rows))
                    for i, n in enumerate(nodecounter):
                        logger.info('%s entries stored at ring id: %s', n, i)
                    logger.info('dht construction complete, signaling manager!')
                    m_sock.sendto(json.dumps({'cmd' : 'dht-complete', 'args' : [name]}).encode(), (manager_ip, manager_port))
                case 'query-dht':
                    
                    event_id = response['event_id']
                    search_name = response['name']
                    search_addr = response['address']
                    search_port = response['p_port']
                    logger.info('forwarding find-event to %s', search_name)
                    req = {'cmd': 'find-event', 'sender_name': name, 'sender_ip': my_ip, 'sender_p_port': p_port, 'event_id': event_id, 'id_seq' : []}
                    p_sock.sendto(json.dumps(req).encode(), (search_addr,int(search_port)))
                
                case 'teardown-dht':
                    
                    
                    
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
                   dhtpeers = response['peers']
                   s = response['s']
                case 'store':     
                    dest_id = response['dest_id']
                    pos = response['pos']
                    row = response['data']
                    
                    if dest_id == ring_id:
                        if pos in table_data:
                            existing = table_data[pos]
                            logger.info('collision at pos %s: existing EVENT_IDS=%s, new EVENT_ID=%s', pos, existing, row['EVENT_ID'])
                        else:
                            table_data[pos] = []
                        table_data[pos].append(row)
                        logger.info('hash valid for this peer, storing...')
                    else:
                        logger.info('directing hash (pos: %s,dest_id %s) to the right neighbor...', pos, dest_id)
                        req = {'cmd': 'store', 'pos': pos, 'dest_id' : dest_id, 'data' : row}
                        p_sock.sendto(json.dumps(req).encode(), (right_neighbor['ip'], int(right_neighbor['p_port'])))

                case 'find-event':
                    find_event(response['sender_name'], response['event_id'], response['id_seq'], response['sender_ip'],  response['sender_p_port'])
                case 'find-event-return':
                    
                    status = response['status']
                    event_id = response['event_id']
                    if int(status) == 1:
                        
                        event = response['event']
                        logger.info("Query success! Storm Event ID %s has attributes: %s", event_id, event)
                    else:
                        logger.info("Query failed! Storm Event ID %s not found. ", event_id)
                case 'teardown':
                    start_name = response['start_name']
                    table_data = {}
                    if name != start_name:
                        logger.info("Tearing down dht, clearing hash table and passing message along.")
                        req = {'cmd': 'teardown', 'start_name' : start_name}
                        p_sock.sendto(json.dumps(req).encode(), (right_neighbor['ip'], int(right_neighbor['p_port'])))
                    else:
                        logger.info("Teardown complete")

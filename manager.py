"""Manager: controls the DHT manager, which handles all queries to the distributed hash table."""

from dataclasses import dataclass
from math import e
import socket
from enum import Enum
from log import logger
import json
import random
import sys



class PeerState(Enum):
    FREE = 0 #  a peer able to participate in any capacity
    LEADER = 1 #  a peer that leads the construction of the DHT
    INDHT = 2 # a peer that is one of the members of the DHT.
    
class DHTState(Enum):
    UNINIT = 0
    CONSTRUCTING = 1
    COMPLETE = 2
    TEARDOWN = 3
    
class returnCodes(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'
    INVALID = 'INVALID'
    
@dataclass
class Peer:
    name: str
    address: str
    m_port: str
    p_port: str
    state: PeerState
    id: int

peers: dict[str, Peer] = {}
dhtpeers: list[Peer] = []
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 6000
IP = "0.0.0.0"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((IP, PORT))

running = True
state = {'dht': DHTState.UNINIT}

logger.info("Server running at %s:%s", IP, PORT)

def registerPeer(name, ip, mport, pport):
    """
    Adds a peer's information to the manager's peer registry.
    
    :param name: a str name of the peer to be registered.
    :param ip: a str ip of the peer's host
    :param mport: a str port for the peer's port used exclusively to talk to the manager
    :param pport: a str port for the peer's port used for everything else (peer communication)
    
    :returns: whether or not the registration was successful.
    
    """ 
    if name in peers:
        return {'status' : returnCodes.FAILURE.value}
    peers[name] = Peer(name, ip, mport, pport, PeerState.FREE, -1)
    return {'status' : returnCodes.SUCCESS.value, 'ip' : ip, 'name' : name}
    
def setup_dht(name : str, size : int, year : str):
    """
    Constructs a DHT 
    
    :param name: a str name of the peer to be declared as the dht leader.
    :param size: a int size of the desired dht
    :param year: a str year to take data from
    
    :returns: whether or not this construction can start.
    """
        
    if state['dht'] != DHTState.UNINIT or name not in peers or size < 3 or len(peers) < size:
        return {'status' : returnCodes.FAILURE.value}

    leader = peers[name]
    peers[name].state = PeerState.LEADER 
    peers[name].id = 0
    dhtpeers.append((leader.name, leader.address, leader.p_port))

    unselected_peers = random.sample([peer for peer in peers.values() if peer.name != name], size-1)
    for i, peer in enumerate(unselected_peers):
        peer.state = PeerState.INDHT
        peer.id = i + 1

        dhtpeers.append((peer.name, peer.address, peer.p_port))
    
    state['dht'] = DHTState.CONSTRUCTING
    return{'status' : returnCodes.SUCCESS.value, 'name' : name, 'year': year, 'peers': [{'name' : peer[0], 'ip': peer[1], 'p_port': peer[2]} for peer in dhtpeers]}

def dht_complete(name : str):
    """
    Signals whether or not the DHT construction completed successfully.
    
    :param name: a str name of the supposed peer leader
    
    :returns: whether or not the dht was constructed correctly
    """
    if name in peers and peers[name].state == PeerState.LEADER:
        state['dht'] = DHTState.COMPLETE
        logger.info('dht construction complete. peers in dht: %s', dhtpeers)

        return {'status' : returnCodes.SUCCESS.value}

    else:
        return {'status' : returnCodes.FAILURE.value}
    
def query_dht(peer_name : str, event_id : str): # event_id is not part of spec but it seems you're supposed to search for it so im adding it here.
    """
    Returns a random peer in the dht
    
    :param peer_name: a str name of the peer to return the results to
    :param event_id: a str name of the 
    :returns: a 3 tuple of information relating to the randomly selected peer
    """
    if peer_name not in peers or state['dht'] == DHTState.UNINIT or peers[peer_name].state != PeerState.FREE:
        reason = "Unknown" # idk this should never be shown logically
        
        if peer_name not in peers:
            reason = "peer not registered!"
        elif state['dht'] == DHTState.UNINIT:
            reason = "DHT not initialized!"
        logger.info('querydht failed! reason: %s', reason)
        return {'status' : returnCodes.FAILURE.value}


    random_peer = random.sample(dhtpeers,1)[0]
    return {'status' : returnCodes.SUCCESS.value, 'sender_name': peer_name, 'event_id': event_id, 'name' : random_peer[0], 'address' : random_peer[1], 'p_port': random_peer[2]}


def leave_dht(peer_name : str): 
    """
    Initiates the process of letting the mentioned peer leave
    
    :param peer_name: name of the peer trying to leave
    """
    if peer_name not in peers or state['dht'] == DHTState.UNINIT or peers[peer_name].state == PeerState.FREE:
        reason = "Unknown" # idk this should never be shown logically
        
        if peer_name not in peers:
            reason = "peer not registered!"
        elif state['dht'] == DHTState.UNINIT:
            reason = "DHT not initialized!"
        logger.info('leave_dht failed! reason: %s', reason)
        return {'status' : returnCodes.FAILURE.value}

    return {'status' : returnCodes.SUCCESS.value}


def teardown_dht(peer_name : str): 
    """
    Initiates the process of tearing down dht
    
    :param peer_name: name of the dht leader
    """
    if peer_name not in peers or state['dht'] == DHTState.UNINIT or peers[peer_name].state != PeerState.LEADER:
        reason = "Unknown" # idk this should never be shown logically
        
        if peer_name not in peers:
            reason = "peer not registered!"
        elif state['dht'] == DHTState.UNINIT:
            reason = "DHT not initialized!"
        logger.info('leave_dht failed! reason: %s', reason)
        return {'status' : returnCodes.FAILURE.value}
    state['dht'] = DHTState.TEARDOWN
    return {'status' : returnCodes.SUCCESS.value}

def teardown_complete(peer_name : str): 
    """
    Teardown signaled as complete, just wrap things up
    
    :param peer_name: name of the dht leader
    """
    if peer_name not in peers or state['dht'] == DHTState.UNINIT or peers[peer_name].state != PeerState.LEADER:
        reason = "Unknown" # idk this should never be shown logically
        
        if peer_name not in peers:
            reason = "peer not registered!"
        elif state['dht'] == DHTState.UNINIT:
            reason = "DHT not initialized!"
        logger.info('leave_dht failed! reason: %s', reason)
        return {'status' : returnCodes.FAILURE.value}

    for peer in dhtpeers:
        peer_name = peer[0]
        peers[peer_name].state = PeerState.FREE
        peers[peer_name].id = -1
    return {'status' : returnCodes.SUCCESS.value}
 
while(running):
    logger.info("current registry state: %s", peers )
    data, addr = sock.recvfrom(1024) 
    
    data_split = json.loads(data.decode())
    cmd = data_split['cmd']
    args = data_split['args'] 
    logger.info("received command %s from address %s", data_split['cmd'], addr)
    res = {'status' : returnCodes.INVALID.value}
    
    if state['dht'] == DHTState.CONSTRUCTING and cmd != "dht-complete": # ignore everything else while we construct
        res =  {'status' : returnCodes.FAILURE.value}
        sock.sendto(json.dumps(res).encode(), addr)
        continue
    elif state['dht'] == DHTState.TEARDOWN and cmd != "teardown-complete": # ignore everything else while we teardown
        res =  {'status' : returnCodes.FAILURE.value}
        sock.sendto(json.dumps(res).encode(), addr)
        continue
    try:
        match(cmd):
            case "register":
                if len(args) != 4:
                    res = {'status' : returnCodes.INVALID.value}
                    
                else:
                    res = registerPeer(args[0], args[1], args[2], args[3])
                
            case "setup-dht":
                if len(args) != 3:
                    res = {'status' : returnCodes.INVALID.value}
                else:
                    res = setup_dht(args[0], int(args[1]), args[2])
            case "dht-complete":
                if len(args) != 1:
                    res = {'status' : returnCodes.INVALID.value}
                else:
                    res = dht_complete(args[0])
            case "query-dht":
                if len(args) != 2:
                    res = {'status' : returnCodes.INVALID.value}
                else:
                    res = query_dht(args[0],args[1])
                
            case _:
                res = {'status' : returnCodes.INVALID.value}
                logger.info("received command %s is invalid!", cmd)
    except (KeyError, ValueError, IndexError) as e:
        logger.error("unexpected error: %s", e)
    res['cmd'] = cmd
    sock.sendto(json.dumps(res).encode(), addr)




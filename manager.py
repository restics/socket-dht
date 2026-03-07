"""Manager: controls the DHT manager, which handles all queries to the distributed hash table."""

from dataclasses import dataclass
import socket
from enum import Enum
from log import logger


class State(Enum):
    FREE = 0 #  a peer able to participate in any capacity
    LEADER = 1 #  a peer that leads the construction of the DHT
    INDHT = 2 # a peer that is one of the members of the DHT.
    
    
class returnCodes(Enum):
    FAILURE = b'FAILURE'
    SUCCESS = b'SUCCESS'
    INVALID = b'INVALID'
    
@dataclass
class Peer:
    name: str
    address: str
    m_port: str
    p_port: str
    state: State

peers: dict[str, Peer] = {}
PORT = 1500
IP = "127.0.0.1"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((IP, PORT))

running = True
dht_constructed = False

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
    logger.info("")
    if name in peers:
        return returnCodes.FAILURE
    peers[name] = Peer(name, ip, mport, pport, State.FREE)
    return returnCodes.SUCCESS
    
def setup_dht(name : str, size : int, year : str):
    """
    Constructs a DHT 
    
    :param name: a str name of the peer to be declared as the dht leader.
    :param size: a int size of the desired dht
    :param year: a str year to take data from
    
    :returns: whether or not this construction can start.
    """
        
    if dht_constructed or name not in peers or size < 3 or len(peers) < size:
        return returnCodes.FAILURE
    return returnCodes.SUCCESS

def dht_complete(name : str):
    """
    Signals whether or not the DHT construction completed successfully.
    
    :param name: a str name of the supposed peer leader
    
    :returns: whether or not the dht was constructed correctly
    """
    
    return returnCodes.SUCCESS if peers[name].state == State.LEADER else returnCodes.FAILURE


while(running):
    
    data, addr = sock.recvfrom(1024) # data comes in a space delimited string of the fields minus the address
    
    data_split = data.decode().split(" ")
    cmd = data_split[0]
    args = data_split[1:]
    
    logger.info("received command %s from address %s with args %s", cmd, addr, args)
    res = returnCodes.FAILURE
    
    match(cmd):
        case "register":
            if len(args != 4):
                break
            res = registerPeer(args[0], args[1], args[2], args[3])
            break
        case "setup_dht":
            if len(args != 3):
                break
            res = setup_dht(args[0], args[1], args[2])
            break
        case "dht_complete":
            if len(args != 3):
                break
            res = dht_complete(args[0])
            break
        case _:
            res = returnCodes.INVALID
            logger.info("received command %s is invalid!", cmd)
    sock.sendto(res.value, addr)




"""
A DHT client that manages key-value pairs through DHT

:Author: Tong (Debby) Ding
:See: CPSC 5510, Seattle University
"""
import sys
import socket
import pickle
import hashlib 

class DHTClient:
    """ A class used to represent a DHT client"""

    def __init__(self, method, key, value):
        self._key = key
        self._value = value
        self._key_hash = self._hash_key()
        self._action = self._get_action(method)
        
    def _hash_key(self):
        """Get the value of key hashed through SHA1

        :rtype: int
        """
        key_hash = hashlib.sha1(str.encode(self._key))
        key_hex = key_hash.hexdigest()
        key_int = int(key_hex, 16)
        return key_int

    def _get_action(self, method):
        """Identify the action on data requested by client

        :param method(str): GET/PUT method indicated by client
        :rtype: str
        """
        if method == "get" and self._value == "":
            return "read"
        elif method == "put" and self._value == "":
            return "delete"
        elif method == "put" and self._value != "":
            return "write"

        print("Invalid action for data management") 
        exit(1)

    def _create_message(self):
        """Create the request message sent to DHT
        
        rtype: bytes
        """
        request = {"action" : self._action, 
                   "key_hash" : self._key_hash, 
                   "key" : self._key, 
                   "value" : self._value}
        return pickle.dumps(request)
        
    def send_request(self, node, node_port):
        """Send request to DHT with given node information

        :param node(str): The hostname of the node
        :param node_port(str): The port number of the node
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            server_addr = (node, node_port)
            s.sendto(self._create_message(), server_addr)

            # set a timeout for getting response
            s.settimeout(3)
            try:
                data, server = s.recvfrom(1024)
                response = pickle.loads(data)
            except socket.timeout:
                print("Timeout for connection to DHT")
                exit(1)

            print("\n---------------------\nClient received msg:\n---------------------")
            for key, value in response.items():
                print("{:10} : {}".format(key, value))
            print()

if __name__ == '__main__':

    value = ""
    if len(sys.argv) == 5:
        _, node, node_port, method, key = sys.argv
    elif len(sys.argv) == 6:
        _, node, node_port, method, key, value = sys.argv
    else:
        print("Usage: python3 dht_client.py node node_port get|put key [value]")
        exit(1)
    
    client = DHTClient(method.lower(), key, value)
    client.send_request(node.lower(), int(node_port))
    exit(0)
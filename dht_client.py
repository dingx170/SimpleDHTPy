"""
A DHT client that manages key-value pairs through DHT
:Author: Tong (Debby) Ding
:See CPSC 5510, Seattle University
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
        self._action = self._get_action(method)
        self._key_hash = self._hash_key()

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

            print("-------\nClient received msg:\n-------\n{}"
                  .format(response))
    
    def _get_action(self, method):
        """Identify the action on data requested by client

        :param method: GET/PUT method indicated by client
        :type method: str
        :rtype: str
        """
        if method == "get" and self._value == "":
            return "read"
        elif method == "put" and self._value == "":
            return "delete"
        elif method == "put" and self._value != "":
            return "write"

        print("Invalid action") 
        exit(1)

    def _create_message(self):
        """Create the request message to DHT"""
        request = {"action" : self._action, 
                   "key_hash" : self._key_hash, 
                   "key" : self._key, 
                   "value" : self._value}
        return pickle.dumps(request)
    
    def _hash_key(self):
        _key_hash = hashlib.sha1(str.encode(self._key))
        key_hex = _key_hash.hexdigest()
        key_int = int(key_hex, 16)
        return key_int

if __name__ == '__main__':

    # python3 dht_client.py node nodePort get|put key [value]
    value = ""
    if len(sys.argv) == 5:
        _, node, node_port, action, key = sys.argv
    elif len(sys.argv) == 6:
        _, node, node_port, action, key, value = sys.argv
    else:
        print("Usage: python3 dht_client.py node nodePort get|put key [value]")
        exit(1)
    
    client = DHTClient(action.lower(), key.lower(), value.lower())
    client.send_request(node.lower(), int(node_port))
    exit(0)
    

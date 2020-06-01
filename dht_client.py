import sys
import socket
import pickle
import hashlib 

class Client:
    def __init__(self, action, key, value):
        self.key = key
        self.value = value
        self.action = self._get_action(action)
        # self.key_hash = self._hash_key()
        self.key_hash = int(key[-1]) # del

    def send_request(self, node, node_port):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            server_addr = (node, node_port)
            s.sendto(self._create_message(), server_addr)
            data, server = s.recvfrom(1024)
            msg_dict = pickle.loads(data)
            print("-------\nClient received msg:\n-------\n{}".format(msg_dict))
    
    def _get_action(self, action):
        if action == "get" and self.value == "":
            return "read"
        elif action == "put" and self.value == "":
            return "delete"
        elif action == "put" and self.value != "":
            return "write"
        else:
            print("invalid action") 

    def _create_message(self):
        msg_dict = {"action" : self.action, "key_hash" : self.key_hash, "key" : self.key, "value" : self.value, "hops" : 0}
        return pickle.dumps(msg_dict)
    
    def _hash_key(self):
        key_hash = hashlib.sha1(self.key.encode())
        key_hex = key_hash.hexdigest()
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
    
    client = Client(action, key, value)
    # client.print_key_id()
    client.send_request(node, int(node_port))
    

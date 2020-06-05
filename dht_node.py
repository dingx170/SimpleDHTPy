"""
A DHT node that manages key-value pairs within a DHT

:Author: Tong (Debby) Ding
:See: CPSC 5510, Seattle University
"""
import sys
import socket
import pickle
import hashlib 

class DHTNode:
    """ A class used to represent a DHT node"""

    def __init__(self, host_file, line_num):
        self._nodes = self._get_nodes(host_file)
        self._node_ids = self._hash_nodes()
        self.host = self._nodes[line_num][0]
        self.port = int(self._nodes[line_num][1])
        self.node_id = self._get_node_id(self.host, self.port)
        self._successor = self._find_successor()
        self._predecessor = self._find_predecessor()
        self._finger_table = self._form_finger_table()
        self.storage = {}
    
    def _get_nodes(self, host_file):
        """Get a list of DHT nodes' hostnames and port numbers from a file

        :param host_file(str): the file containing node information
        :rtype: list of tuples (host, port)
        """ 
        with open(host_file) as f:
            nodes = []
            for line in f:
                name, port = line.split(' ')
                host = socket.gethostbyname(name)
                nodes.append((host, port))
        return nodes
    
    def _hash_nodes(self):
        """Get the nodes' hashed values as their IDs
        
        :rtype: dict of node_id : node address (k:v)
        """
        nodes = {}
        for node in self._nodes:
            host, port = node
            node_id = self._get_node_id(host, port)
            nodes[node_id] = node
        return nodes

    def _get_node_id(self, host, port):
        """Get the node's ID by hashing its host and port with SHA1
        
        :rtype: int
        """
        host_b = socket.inet_pton(socket.AF_INET, host)
        port_b = int(port).to_bytes(2, byteorder = "big")
        node_id_hash = hashlib.sha1(host_b + port_b)
        node_id_hex = node_id_hash.hexdigest()
        node_id_int = int(node_id_hex, 16)
        return node_id_int

    def _find_successor(self):
        """Get the successor of a node
        
        rtype: str
        """
        node_ids = list(self._node_ids.keys())
        node_ids.sort()

        if node_ids[-1] == self.node_id:
            return node_ids[0]
        
        pos = node_ids.index(self.node_id)
        return node_ids[pos + 1]

    def _find_predecessor(self):
        """Get the predecessor of a node
                
        rtype: str
        """
        node_ids = list(self._node_ids.keys())
        node_ids.sort()

        if node_ids[0] == self.node_id:
            return node_ids[-1]
        
        pos = node_ids.index(self.node_id)
        return node_ids[pos - 1]
    
    def _form_finger_table(self):
        """Generate the finger table for DHT
        
        :rtype: dict
        """
        m = 160
        node_entries = [(self.node_id + 2**(k-1))%(2**m) for k in range(1, m+1)]
        node_ids = list(self._node_ids.keys())

        node_entries.sort()
        node_ids.sort()
        finger_table = {}

        i = j = 0
        while (i < len(node_entries)) and j < len(node_ids):
            if node_entries[i] <= node_ids[j]:
                finger_table[node_entries[i]] = node_ids[j]
                i = i + 1
            else:
                j = j + 1
                
        while i < len(node_entries):
            finger_table[node_entries[i]] = node_ids[0]
            i = i + 1
    
        return finger_table

    def run(self):
        """Activate the DHT node to receive and process requests"""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, self.port))

            while True:
                data, address = s.recvfrom(1024)
                request = pickle.loads(data)

                # check if this node is the first node the client contacts
                if "hops" not in request:
                    request["hops"] = 0
                    request["client"] = address

                self._print_rcvd_msg(request)

                # find the proper node to forward the request to
                node_id = self._find_fwd_node(request)
                response = {}

                # reply to client if this node matches with the hashed key 
                if node_id == self.node_id:
                    response, address = self._process_request(request)
                    s.sendto(pickle.dumps(response), address)
                    print(response)

                # otherwise, forward request to the proper node
                else:
                    host, port = self._node_ids[node_id]
                    request["hops"] = request["hops"] + 1
                    s.sendto(pickle.dumps(request), (host, int(port)))

    def _process_request(self, request):
        """Process the received request
        
        :rtype: dict
        """
        action = request["action"]
        address = request["client"]
        del request["client"]
        request["status"] = "OK"
        request["hops"] = request["hops"] + 1
        request["from_node"] = self.node_id
        
        if action == "read":
            try:
                request["value"] = self.storage[request["key"]]
            except KeyError:
                request["status"] = "error: key not found"

        elif action == "write":
            self.storage[request["key"]] = request["value"]

        elif action == "delete":
            try:
                self.storage.pop(request["key"])
            except KeyError:
                request["status"] = "error: key not found"

        return request, address

    def _print_rcvd_msg(self, request):
        """Display the node info and received message"""
        print("\n---------------------\nDHT node info:\n---------------------")
        print("* predecessor: {}".format(self._predecessor))
        print("* curr_node  : {}".format(self.node_id))
        print("* successor  : {}".format(self._successor))
        print("-> rcvd key  : {}".format(request["key_hash"]))
        print("---------------------\nDHT node storage:\n---------------------")
        for key, val in self.storage.items():
            print("{} : {}".format(key, val))
        print("---------------------\nDHT node rcvd msg:\n---------------------")
        print("action  : {}".format(request["action"]))  
        print("key     : {}".format(request["key"]))
        print("value   : {}".format(request["value"]))
        print("hops    : {}".format(request["hops"]))      
        print("---------------------\n")

    def _find_fwd_node(self, request):
        """Find the proper node to forward the request
        
        rtype: str
        """
        key = request["key_hash"]

        if key == self._predecessor:
            print("----> key goes to predecessor")
            return self._predecessor

        if self._predecessor > self.node_id:
            if self._predecessor < key or key <= self.node_id:
                print("----> key goes to me")
                return self.node_id
        else: 
            if self._predecessor < key <= self.node_id:
                print("----> key goes to me")
                return self.node_id

        if self._successor < self.node_id:
            if self.node_id < key or key <= self._successor:
                print("----> key goes to successor")
                return self._successor
        else:
            if self.node_id < key <= self._successor:
                print("----> key goes to successor")
                return self._successor
            
        if key > self._successor:
            print("----> check finger table")
            table = self._finger_table
            nodes = list(table.keys())         
                         
            if key >= nodes[-1]:
                return table[nodes[-1]]
            else: 
                for i in range(len(nodes) - 1, 0, -1):
                    print("--- check range: {} - {}".format(nodes[i-1], nodes[i]))
                    if nodes[i-1] <= key < nodes[i]:
                        print("--> key goes to {} : {}".format(nodes[i-1], table[nodes[i-1]]))
                        return table[nodes[i-1]]
        
        print("----> key goes to predecessor")
        return self._predecessor

if __name__ == '__main__':

    if len(sys.argv) == 3:
        _, host_file, line_num = sys.argv
    else:
        print('Usage: python3 dht_node.py host_file line_num')
        exit(1)
    
    try:
        line_num = int(line_num)
    except ValueError as ex:
        print('{} is not a digit'.format(line_num))
        exit(1)

    node = DHTNode(host_file.lower(), line_num)
    node.run()
    exit(0)





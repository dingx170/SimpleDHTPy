import sys
import socket
import pickle
import hashlib 

class DHTNode:
    def __init__(self, host_file, line_num):
        self._nodes = self._get_nodes(host_file)
        self._hashed_nodes = self._hash_nodes()
        self.host = self._nodes[line_num][0]
        self.port = int(self._nodes[line_num][1])
        self.node_id = self._get_node_id(self.host, self.port)
        # self.node_id = int(self._nodes[line_num][1][-2]) # del
        self._succ = self._find_succ()
        self._pred = self._find_pred()
        self._finger_table = self._prep_table()
        self.storage = {}
    
    def _get_nodes(self, host_file):
        """Get a list of DHT nodes name and IP number from a file

        :param host_file(str): the file containing node information
        :rtype: list of tuples
        """ 
        with open(host_file) as f:
            nodes = []
            for line in f:
                name, port = line.split(' ')
                ip = socket.gethostbyname(name)
                nodes.append((ip, port))
        return nodes
    
    # fix-me
    def _hash_nodes(self):
        """Get a dictionary of the nodes' hash value and address"""
        nodes = {}
        for node in self._nodes:
            ip, port = node
            node_id = self._get_node_id(ip, port)
            # node_id = int(str(port)[-2]) # del
            nodes[node_id] = node
        return nodes

    def run(self):
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

                # reply to client if this node maches the hashed key 
                if node_id == self.node_id:
                    response = self._process_request(request)
                    s.sendto(pickle.dumps(response), request["client"])
                    print(response)

                # otherwise, forward request to the proper node
                else:
                    ip, port = self._hashed_nodes[node_id]
                    request["hops"] = request["hops"] + 1
                    s.sendto(pickle.dumps(request), (ip, int(port)))

    def _process_request(self, request):
        action = request["action"]
        request["status"] = "OK"
        request["hops"] = request["hops"] + 1
        
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

        return request

    def _find_succ(self):
        """Get the successor of a node"""
        node_ids = list(self._hashed_nodes.keys())
        node_ids.sort()

        if node_ids[-1] == self.node_id:
            return node_ids[0]
        
        pos = node_ids.index(self.node_id)
        return node_ids[pos + 1]

    def _find_pred(self):
        """Get the predecessor of a node"""
        node_ids = list(self._hashed_nodes.keys())
        node_ids.sort()

        if node_ids[0] == self.node_id:
            return node_ids[-1]
        
        pos = node_ids.index(self.node_id)
        return node_ids[pos - 1]

    # fix-me
    def _prep_table(self):
        m = 160 # del 4
        node_entries = [(self.node_id + 2**(k-1))%(2**m) for k in range(1, m+1)]

        node_ids = list(self._hashed_nodes.keys())

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
        
        print("* finger table")
        for key, val in finger_table.items():
            print("   {} : {}".format(key, val))
        return finger_table

    def _get_node_id(self, host, port):
        host_b = socket.inet_pton(socket.AF_INET, host)
        port_b = int(port).to_bytes(2, byteorder = "big")
        node_id_hash = hashlib.sha1(host_b + port_b)
        node_id_hex = node_id_hash.hexdigest()
        node_id_int = int(node_id_hex, 16)
        return node_id_int
    
    def _print_rcvd_msg(self, request):
        print("---------------------\nDHT node {} information:\n---------------------".format(self.node_id))
        print("* predecessor: {}".format(self._pred))
        print("* my_node  : {}".format(self.node_id))
        print("* successor  : {}".format(self._succ))
        print("---------------------\nDHT node {} rcvd msg:\n---------------------".format(self.node_id))
        print("action : {}".format(request["action"]))
        print("keyhash: {}".format(request["key_hash"]))
        print("key    : {}".format(request["key"]))
        print("value  : {}".format(request["value"]))
        print("hops   : {}".format(request["hops"]))
        print("---------------------\nDHT node {} storage:\n---------------------".format(self.node_id))
        for key, val in self.storage.items():
            print("{} : {}".format(key, val))
        print("---------------------")
        print("PRINT --- PRED --- KEY --- NODE_ID --- SUCC")
        print(self._pred, request["key_hash"], self.node_id, self._succ)
        print()
        print()
        print()

    def _find_fwd_node(self, request):
        key = request["key_hash"]

        if key == self._pred:
            print("----> key goes to predecessor")
            return self._pred
            
        if self._pred > self.node_id:
            if self._pred < key or key <= self.node_id:
                print("----> key goes to me")
                return self.node_id
        else: 
            if self._pred < key <= self.node_id:
                print("----> key goes to me")
                return self.node_id

        if self._succ < self.node_id:
            if self.node_id < key or key <= self._succ:
                print("----> key goes to successor")
                return self._succ
        else:
            if self.node_id < key <= self._succ:
                print("----> key goes to successor")
                return self._succ
            
        if key > self._succ:
            print("----> check finger table <----")
            table = self._finger_table
            nodes = list(table.keys())         
                         
            if key >= nodes[-1]:
                return table[nodes[-1]]
            else: 
                for i in range(len(nodes) - 1, 0, -1):
                    print("compare range: {} - {}".format(nodes[i-1], nodes[i]))
                    if nodes[i-1] <= key < nodes[i]:
                        print("--> key goes to {} : {}".format(nodes[i-1], table[nodes[i-1]]))
                        return table[nodes[i-1]]
        
        print("----> key goes to predecessor<----")
        return self._pred

if __name__ == '__main__':

    # python3 dht_node.py host_file line_num
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
    





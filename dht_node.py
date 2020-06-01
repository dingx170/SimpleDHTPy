import sys
import socket
import pickle
import hashlib 

class DHTNode:
    def __init__(self, host_file, line_num):
        self.nodes = self._get_nodes(host_file)
        self.host = self.nodes[line_num][0]
        self.port = int(self.nodes[line_num][1])
        # self.node_id = self._get_node_id()
        self.node_id = int(self.nodes[line_num][1][-2]) # del
        self.storage = {"Oregon4" : 1234} # fix-me
        self._successor = self._find_successor()
        self._predecessor = self._find_predecessor()
        self._finger_table = self._prep_table()
    
    def _get_nodes(self, host_file):
        with open(host_file) as f:
            nodes = []
            for line in f:
                name, port = line.split(' ')
                ip = socket.gethostbyname(name)
                nodes.append((ip, port))
        return nodes

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, self.port))
            while True:
                data, address = s.recvfrom(1024)
                request = pickle.loads(data)
                print("---------------------\nDHT node {} rcvd msg:\n---------------------".format(self.node_id))
                print("action : {}".format(request["action"]))
                print("keyhash: {}".format(request["key_hash"]))
                print("key    : {}".format(request["key"]))
                print("value  : {}".format(request["value"]))
                print("hops   : {}".format(request["hops"]))
                print("---------------------\nmy storage:\n---------------------")
                for key, val in self.storage.items():
                    print("{} : {}".format(key, val))
                print("---------------------")

                response = {}
                if request["key_hash"] == self._predecessor:
                    print("----> key goes to predecessor")
                    break
                elif self._predecessor < request["key_hash"] <= self.node_id:
                    print("----> key goes to me")
                    response = self._process_request(request) 
                    print(response)

                elif self.node_id < request["key_hash"] <= self._successor:
                    print("----> key goes to successor")
                    break
                else:
                    print("----> check finger table <----")
                    finger_table = self._finger_table

                    node_entries = list(finger_table.keys())                      
                    if request["key_hash"] >= node_entries[-1] or request["key_hash"] < self._predecessor:
                        print("--> key goes to {} : {}".format(node_entries[-1], finger_table[node_entries[-1]]))
                    else: 
                        for i in range(len(node_entries) - 1, 0, -1):
                            print("compare range: {} - {}".format(node_entries[i-1], node_entries[i]))
                            if node_entries[i-1] <= request["key_hash"] < node_entries[i]:
                                print("--> key goes to {} : {}".format(node_entries[i-1], finger_table[node_entries[i-1]]))
                                break
                print("\n")

                s.sendto(pickle.dumps(response), address)
                # show_storage()
    
    def _process_request(self, request):
        request["status"] = "OK"
        if request["action"] == "read":
            try:
                request["value"] = self.storage[request["key"]]
            except KeyError:
                request["status"] = "error: key not found"
  
        elif request["action"] == "write":
            self.storage[request["key"]] = request["value"]
            
        elif request["action"] == "delete":
            try:
                self.storage.pop(request["key"])
            except KeyError:
                request["status"] = "error: key not found"


        request["hops"] = request["hops"] + 1

        return request

    # def _form_response(self, action, key_hash)

    def _find_successor(self):
        nodes = self._hash_nodes()
        node_ids = list(nodes.keys())
        node_ids.sort()

        if node_ids[-1] == self.node_id:
            return node_ids[0]
        
        pos = node_ids.index(self.node_id)

        print("\n------------------")
        print("* successor  : {}".format(node_ids[pos + 1]))
        return node_ids[pos + 1]

    def _find_predecessor(self):
        nodes = self._hash_nodes()
        node_ids = list(nodes.keys())
        node_ids.sort()

        if node_ids[0] == self.node_id:
            return node_ids[-1]
        
        pos = node_ids.index(self.node_id)
        
        print("* predecessor: {}".format(node_ids[pos - 1]))
        print("------------------")
        return node_ids[pos - 1]


    # fix-me
    def _prep_table(self):
        m = 4
        node_entries = [(self.node_id + 2**(k-1))%(2**m) for k in range(1, m+1)]
        nodes = self._hash_nodes()
        node_ids = list(nodes.keys())

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





    # fix-me
    def _hash_nodes(self):
        nodes = {}
        for node in self.nodes:
            ip, port = node
            node_id = int(str(port)[-2])
            nodes[node_id] = node
        return nodes


    # def _check_id(self, ):
              
    def _get_node_id(self):
        data = self.host + str(self.port)
        node_id_hash = hashlib.sha1(data.encode())
        node_id_hex = node_id_hash.hexdigest()
        node_id_int = int(node_id_hex, 16)
        return node_id_int

    # def show_storage(self):
    #     print(self.storage)

    def print_nodes(self):
        for node in self.nodes:
            print(node)
    
    # def print_me(self):
    #     print("{}, {}".format(self.host, self.port))
    
    def print_id(self):
        print("node_id : {}".format(self.node_id))

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

    node = DHTNode(host_file, line_num)
    # node.print_id()
    # node.print_nodes()
    
    node.run()
    





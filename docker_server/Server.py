import socketserver
import socket
import json
import os
import time
import threading

# Read and write dictionary to a local file.
def read_data(path):
    with open(path) as file:
        return json.load(file)

def write_data(path, map):
    with open(path, 'w') as file:
        json.dump(map, file)

# Process the request based on its operation. 
def operation(data, data_path, server):
    if data[0] == 'put':
        server.dataset[data[1]] = data[2]
        msg = data[1]
        return msg
    elif data[0] == 'get':
        try:
            msg = server.dataset[data[1]]
            return msg
        except KeyError:
            msg = 'Can\'t find the key {}'.format(data[1])
            return msg
    elif data[0] == 'del':
        try:
            result = server.dataset.pop(data[1])
            msg = 'delete value {}'.format(result)
            return msg
        except KeyError:
            msg = 'Can\'t find the key {}'.format(data[1])
            return msg
    elif data[0] == 'store':
        return json.dumps(server.dataset)
    elif data[0] == 'exit':
        msg = 'server shutdown'
        return msg
    elif data[0] == 'dput1':
        if data[1] in server.lock_list:
            return "abort"
        else:
            server.lock_list.append(data[1])
            return "acknowledge"
    elif data[0] == 'dput2':
        data_dput2 = data[:]
        data_dput2[0] = "put"
        msg = operation(data_dput2, data_path, server)
        server.lock_list.remove(data[1])
        return msg
    elif data[0] == 'dputabort':
        server.lock_list.remove(data[1])
        return "abort"
    elif data[0] == 'ddel1':
        if data[1] in server.lock_list:
            return "abort"
        else:
            server.lock_list.append(data[1])
            return "acknowledge"
    elif data[0] == 'ddel2':
        data_ddel2 = data[:]
        data_ddel2[0] = "del"
        msg = operation(data_ddel2, data_path, server)
        server.lock_list.remove(data[1])
        return msg
    elif data[0] == 'ddelabort':
        server.lock_list.remove(data[1])
        return "abort"

def ConfigFile(path, server):
    while True:
        members = {}
        with open(path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                addr_port = line.split(':')
                members[addr_port[0]] = addr_port[1]
            # print('main thread: {}'.format(members))
            server.members = members
        time.sleep(3)

def udp_listen(send_addr):
    while True:
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        _, port = send_addr
        msg = str(port)
        sender.sendto(msg.encode('utf-8'), ('<broadcast>', 4410))
        time.sleep(0.1)

def update_members(map):
    members = []
    for ip in map:
        members.append(str(ip)+":"+str(map[ip][1]))
    return members

def udp_check(map, server):
    while True:
        #print(map)
        for ip in list(map):
            map[ip][0] -= 1
            if map[ip][0] == 0:
                map.pop(ip)
                print('delete ip: {}'.format(ip))
        #server.members = update_members(map)
        time.sleep(1)

def UdpDiscover(send_addr, server):
    listen_thread = threading.Thread(target=udp_listen, 
                    args=(send_addr,),
                    daemon=True)
    listen_thread.start()

    check_thread = threading.Thread(target=udp_check, 
                args=(server.members_udp, server),
                daemon=True)
    check_thread.start()
    while True:
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        listener.bind(("",4410))
        addr_port = listener.recvfrom(1024)
        port, (addr, _) = addr_port
        port = int(port)
        #print("Udp receive addr: {}:{}".format(addr, port))
        server.members_udp[addr] = []
        server.members_udp[addr].append(30)
        server.members_udp[addr].append(port)
        server.members = update_members(server.members_udp)
        print(server.members)
        time.sleep(1)
        
def maintain_membership(com_type,server):
    if com_type == 'udp':
        UdpDiscover(server.server_address, server)
    else:
        ConfigFile(com_type, server)

# Convert data list to string
def data_to_string(data):
    data_string = ""
    for i in range(len(data)):
        if i < len(data)-1:
            data_string = data_string + data[i] + " "
        else:
            data_string = data_string + data[i]
            
    return data_string
    
        
def restore(member_index, data_abort, server):
    # No data needs to restore if index is zero
    if member_index == 0:
        print("No node restored!")
        return "No node restored!"
    else:
        for i in range(member_index-1):
            addr_port = server.members[i].split(":")
            addr = addr_port[0]
            port = int(addr_port[1])
            
            if addr == server.server_address[0] and port == int(server.server_address[1]):
                msg = operation(data_abort, data_abort, server)
                continue
            
            # Connect members with TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
            sock.sendall(bytes(data_to_string(data_abort) + "\n", "utf-8"))
            
        print("Successfully restored!")
        return "Successfully restored!"        
            
# Algorithm phase one
def phase_one(data, data_path, server):
    data_phase_one = data[:]
    
    # Change operation to dput or ddel
    if data[0] == "put":
        data_phase_one[0] = "dput1"
    elif data[0] == "del":
        data_phase_one[0] = "ddel1"
    
    print("before phase one loop")
    # Traverse all the members to commit the operation
    for i in range(len(server.members)):
        print("phase one loop {}".format(i))
        addr_port = server.members[i].split(':')
        addr = addr_port[0]
        port = int(addr_port[1])

        # If current member is leader itself
        if addr == server.server_address[0] and port == int(server.server_address[1]):
            print("phase one leader operation")
            msg = operation(data_phase_one, data_path, server)
            
            # If abort, try 10 times to make sure it's true abort
            try_cnt = 0
            while (msg == "abort" and try_cnt < 10):
                msg = operation(data_phase_one, data_path, server)
                try_cnt = try_cnt + 1
                
            if (msg == "abort"):
                data_abort = data[:]
                if data[0] == "put":
                    data_abort[0] = "dputabort"
                elif data[0] == "del":
                    data_abort[0] = "ddelabort"
                
                restore(i, data_abort, server)
                print("phase one fail")
                return "abort"
        else:
            print("phase one member operation")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
            sock.sendall(bytes(str(data_phase_one) + "\n", "utf-8"))
            msg = str(sock.recv(1024), "utf-8")
            
            # If abort, try 10 times to make sure it's true abort
            try_cnt = 0
            while (msg == "abort" and try_cnt < 10):
                sock.sendall(bytes(str(data_phase_one) + "\n", "utf-8"))
                msg = str(sock.recv(1024), "utf-8")
                try_cnt = try_cnt + 1
                
            if (msg == "abort"):
                data_abort = data[:]
                if data[0] == "put":
                    data_abort[0] = "dputabort"
                elif data[0] == "del":
                    data_abort[0] = "ddelabort"
                
                restore(i, data_abort, server)
                print("phase one fail")
                return "abort"
            
    print("phase one success")
    return "phase one success"
        
# Algorithm phase two
def phase_two(data, data_path, server):
    data_phase_two = data[:]
    
    # Change operation to dput or ddel
    if data[0] == "put":
        data_phase_two[0] = "dput2"
    elif data[0] == "del":
        data_phase_two[0] = "ddel2"
    
    print("before phase two loop")
    # Traverse all the members to commit the operation
    for i in range(len(server.members)):
        print("phase two loop {}".format(i))
        addr_port = server.members[i].split(':')
        addr = addr_port[0]
        port = int(addr_port[1])
        
        # If current member is leader itself
        if addr == server.server_address[0] and port == int(server.server_address[1]):
            print("phase two leader operation")
            msg = operation(data_phase_two, data_path, server)
        else:
            print("phase two member operation")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
            sock.sendall(bytes(data_to_string(data_phase_two) + "\n", "utf-8"))
            msg = str(sock.recv(1024), "utf-8")
           
    print("phase two success") 
    return msg

# Handler classes for TCP and UDP
class MyTCPHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.data_path = './temp.json'
        socketserver.BaseRequestHandler.__init__(
            self, request, client_address, server)
        return

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print("{} wrote:".format(self.client_address[0]))
        print(self.data)
        
        data = self.data.decode("utf-8")
        data = data.split(' ')
        data_path = self.data_path
        server = self.server
        
        if data[0] == "put" or data[0] == "del":
            # Elect a leader here
            msg_phase_one = phase_one(data, data_path, server)
            if msg_phase_one == "abort":
                msg = "Request abort!"
            elif msg_phase_one == "phase one success":
                msg_phase_two = phase_two(data, data_path, server)
                msg = msg_phase_two
        else:
            msg = operation(data, data_path, server)
            
        self.request.sendall(bytes(msg + "\n", "utf-8"))
        if msg == 'server shutdown':
            os.system('kill %d'%os.getpid())

class MyUDPHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.data_path = './temp.json'
        socketserver.BaseRequestHandler.__init__(
            self, request, client_address, server)
        return

    def handle(self):
        # self.request is the TCP socket connected to the client
        data = self.request[0].strip()
        socket = self.request[1]
        print("{} wrote:".format(self.client_address[0]))
        print(data)

        data = data.decode("utf-8")
        data = data.split(' ')
        msg= operation(data, self.data_path, self.server)
        socket.sendto(msg, self.client_address)
        if msg.decode('utf-8') == 'server shutdown':
            os.system('kill %d'%os.getpid())
        
# Helper class for TCP and UDP server
class MyTcpServer(socketserver.ThreadingTCPServer):
    def __init__(self, server_addr, Handler):
        socketserver.TCPServer.__init__(self, server_addr, Handler)
        self.members = []
        self.lock_list = []
        self.dataset = {}
        self.members_udp = {}

class MyUdpServer(socketserver.ThreadingUDPServer):
    conti = True
    def __init__(self, server_addr, Handler):
        socketserver.UDPServer.__init__(self, server_addr, Handler)

if __name__ == "__main__":
    import sys
    # Help information
    if len(sys.argv[1:]) <= 0:
        print('us/ts <port> UDP/TCP/TCP-and-UDP SERVER: run server on <port>.\ntus <tcpport>\
         <udpport> TCP-and-UDP SERVER: run servers on <tcpport> and <udpport> \
             sharing same key-value store.')
        sys.exit()
    HOST, PORT = "", int(sys.argv[2])
    protocol = sys.argv[1]
    print(protocol, HOST, PORT)

    # Create the server, binding to 0.0.0.0 on designed port
    
    if protocol == 'tc':
        server = MyTcpServer((HOST, PORT), MyTCPHandler)
        print('tcp server created ....')
    else:
        server = MyUdpServer((HOST, PORT), MyUDPHandler)
        print('udp server created ....')

    membership_thread = threading.Thread(target=maintain_membership, 
                    #args=('/home/ubuntu/assignment2/test.cfg',server,),
                    args=('udp',server,),
                    daemon=True)
    membership_thread.start()

    '''
    def test_fun():
        while True:
            print('test threading: {}'.format(server.members))
            time.sleep(5)
    mem_test_thread = threading.Thread(target=test_fun, daemon=True)
    mem_test_thread.start()
    '''

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()    
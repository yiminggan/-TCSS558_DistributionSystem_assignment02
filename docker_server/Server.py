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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ###### connect members with TCP ###############
        '''
        for i in membership_list:
            sock.connect(i, mmebership_list[i])
            member_msg = 'dput1'
            sock.sendall(bytes(command + "\n", "utf-8"))
            received = str(sock.recv(65100), "utf-8")
        '''
        msg = ''
        return msg
    elif data[0] == 'get':
        try:
            map = read_data(data_path)
            msg = map[data[1]].encode('utf-8')
            return msg
            # request.sendall(map[data[1]].encode('utf-8'))
        except KeyError:
            msg = 'Can\'t find the key {}'.format(data[1]).encode('utf-8')
            return msg
    elif data[0] == 'del':
        try:
            map = read_data(data_path)
            result = map.pop(data[1])
            write_data(data_path, map)
            msg = 'delete value {}'.format(result).encode('utf-8')
            return msg
        except KeyError:
            msg = 'Can\'t find the key {}'.format(data[1]).encode('utf-8')
            return msg
        # request.sendall(msg.encode('utf-8'))
    elif data[0] == 'store':
        # map = read_data(data_path)
        # msg = json.dumps(map).encode('utf-8')
        # if len(msg) >=65000:
        #     msg = b'TRIMMED: '+msg[0:65000]
        return json.dumps(server.members).encode('utf-8')
        # request.sendall(msg)
    elif data[0] == 'exit':
        # request.sendall('server shutdown'.encode('utf-8'))
        os.remove(data_path)
        msg = 'server shutdown'.encode('utf-8')
        return msg
    elif data[0] == 'dput1':
        # implementation need
        return None
    elif data[0] == 'dput2':
        '''
        lock = threading.RLock()
        with lock:
            # change the key/value storage
        '''
        return None
    elif data[0] == 'dputabort':
        # implementation need
        return None

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

def udp_receive(send_addr):
    while True:
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        _, port = send_addr
        msg = str(port)
        sender.sendto(msg.encode('utf-8'), ('<broadcast>', 4410))
        time.sleep(0.1)

def udp_check(map):
    while True:
        print(map)
        for ip in list(map):
            map[ip] -= 1
            if map[ip] == 0:
                map.pop(ip)
                print('delete ip: {}'.format(ip))
        time.sleep(1)

def UdpDiscover(send_addr, server):
    listen_thread = threading.Thread(target=udp_receive, 
                    args=(send_addr,),
                    daemon=True)
    listen_thread.start()

    check_thread = threading.Thread(target=udp_check, 
                args=(server.members_udp,),
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
        server.members_udp[addr] = 30
        time.sleep(1)
        
def maintain_membership(com_type,server):
    if com_type == 'udp':
        UdpDiscover(server.server_address, server)
    else:
        ConfigFile(com_type, server)


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
        msg = operation(data, self.data_path, self.server)
        self.request.sendall(msg)
        if msg.decode('utf-8') == 'server shutdown':
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
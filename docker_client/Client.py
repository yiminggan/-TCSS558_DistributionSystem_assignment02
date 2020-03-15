import socket
import sys
import threading
import Pyro4
import serpent

if len(sys.argv[1:]) <= 0:
    print('Help information need....')
elif len(sys.argv[1:]) == 1 and sys.argv[1] == 'stop':
    with open('/stop.txt', 'w') as f:
        f.write('exit client program .....')
    sys.exit()
protocol = sys.argv[1]
HOST = sys.argv[2]
PORT = int(sys.argv[3])
# print(protocol, HOST, PORT)
command = " ".join(sys.argv[4:])

# Create a socket 
# SOCK_STREAM means a TCP socket
# SOCK_DGRAM means a UDP socket
if protocol == 'tc':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        sock.sendall(bytes(command + "\n", "utf-8"))

        # Receive data from the server
        received = str(sock.recv(65100), "utf-8")
    finally:
        sock.close()
elif protocol == 'rmic':
    uri = 'PYRO:node.request@'+HOST+':'+str(PORT)
    warehouse = Pyro4.Proxy(uri)
    warehouse.put_values(command, './temp.json')
    msg = warehouse.operation()
    received = serpent.tobytes(msg).decode('utf-8')
else:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Connect to server and send data
        sock.sendto(bytes(command + "\n", "utf-8"), (HOST, PORT))

        # Receive data from the server and shut down
        received = str(sock.recv(65100), "utf-8")
    finally:
        sock.close()


#print("Sent:     {}".format(command))
print(received)
import socket
import sys

protocol = sys.argv[1]
HOST = sys.argv[2]
PORT = int(sys.argv[3])
print(protocol, HOST, PORT)

# HOST, PORT = "localhost", 9998
command = " ".join(sys.argv[4:])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.send(command.encode('utf-8'))
data = s.recv(1024)
s.close()

print('received data: ', data)
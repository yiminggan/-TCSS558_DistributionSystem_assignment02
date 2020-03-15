import socket
import sys

HOST, PORT = "", int(sys.argv[2])
protocol = sys.argv[1]
print(protocol, HOST, PORT)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = (HOST, PORT)
sock.bind(server_address)

sock.listen(1)
connection, client_address = sock.accept()
while True:
    data = connection.recv(20)
    if not data: break
    print('received data: ', data)
    connection.send(data.upper())
connection.close()
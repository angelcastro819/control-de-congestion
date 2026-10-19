import sys
import SocketTCP
import os

host = sys.argv[1]
port = int(sys.argv[2])

server_socketTCP = SocketTCP.SocketTCP()
server_socketTCP.bind((host, port))
connection_socketTCP, new_address = server_socketTCP.accept()

while True:
    data = connection_socketTCP.recv(64, mode=os.environ.get("TCP_MODE", "stop_and_wait"))
    if data == b"": # b"" significa que llegó el FIN
        break
    sys.stdout.buffer.write(data)
sys.stdout.buffer.flush()

connection_socketTCP.recv_close()
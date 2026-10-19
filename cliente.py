import sys
import SocketTCP
import os

host = sys.argv[1]
port = int(sys.argv[2])

data = sys.stdin.buffer.read()

client_socketTCP = SocketTCP.SocketTCP()
client_socketTCP.connect((host, port))
client_socketTCP.send(data, mode=os.environ.get("TCP_MODE", "stop_and_wait"))
client_socketTCP.close()
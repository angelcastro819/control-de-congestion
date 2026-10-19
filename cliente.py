import sys
import SocketTCP
import os
import time

host = sys.argv[1]
port = int(sys.argv[2])
data = sys.stdin.buffer.read()

client_socketTCP = SocketTCP.SocketTCP()
client_socketTCP.connect((host, port))

start = time.perf_counter()
client_socketTCP.send(data, mode=os.environ.get("TCP_MODE", "stop_and_wait"))
elapsed = time.perf_counter() - start

print(
    f"Tiempo: {elapsed:.6f} s | "
    f"Segmentos enviados: {client_socketTCP.number_of_sent_segments}",
    file=sys.stderr
)

client_socketTCP.close()
from socketUDP import SocketUDP
from slidingWindowCC import SlidingWindowCC
import socket
import random
import os
import time
import sys

DEBUG = os.environ.get("TCP_DEBUG") == "1"
LOSS_RATE = float(os.environ.get("TCP_LOSS", "0"))

def debug(msg):
    if DEBUG:
        print("[DEBUG]", msg, file=sys.stderr, flush=True)

class _LossySocket:
    """Envuelve el socket UDP para simular pérdida al enviar (TCP_LOSS)"""
    def __init__(self, sock):
        self.sock = sock

    def sendto(self, data, address):
        if LOSS_RATE > 0 and random.random() < LOSS_RATE:
            debug("PERDIDO al enviar (GBN)")
            return
        self.sock.sendto(data, address)

    def recvfrom(self, buffer_size):
        return self.sock.recvfrom(buffer_size)

    def close(self):
        self.sock.close()

class SocketTCP:
    def __init__(self):
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.dest_address = None
        self.seq_num = 0
        self.expected_seq_num = 0
        self.timeout = float(os.environ.get("TCP_TIMEOUT", "2.0"))
        self.chunk_size = 16
        self.window_size = 8
        self.recv_buffer = b""
        self.remaining_bytes = 0
        self.fin_seq = None

    @staticmethod
    def create_segment(segment):
        flags = bytes([int(segment["ack"]), int(segment["syn"]), int(segment["fin"])])
        seq_bytes = segment["seq"].to_bytes(4, byteorder="big")
        data = segment["data"]

        if len(data) > 16:
            raise ValueError("Los datos no pueden superar los 16 bytes")

        return flags + seq_bytes + data

    @staticmethod
    def parse_segment(segment):
        if not 7 <= len(segment) <= 23:
            raise ValueError("Tamaño de segmento inválido")

        return {
            "ack": bool(segment[0]),
            "syn": bool(segment[1]),
            "fin": bool(segment[2]),
            "seq": int.from_bytes(segment[3:7], byteorder="big"),
            "data": segment[7:]
        }

    def bind(self, address):
        self.udp_socket.bind(address)

    def connect(self, address):
        #secuencia inicial aleatoria
        self.seq_num = random.randint(0, 100)

        #primer mensaje: SYN
        syn = {
            "ack": False,
            "syn": True,
            "fin": False,
            "seq": self.seq_num,
            "data": b""
        }
        previous_timeout = self.udp_socket.gettimeout()
        self.udp_socket.settimeout(self.timeout)
        try:
            connected = False
            while not connected:
                self._sendto(self.create_segment(syn), address)
                try:
                    while True:
                        response_bytes, server_address = self.udp_socket.recvfrom(23)
                        response = self.parse_segment(response_bytes)
                        if (response["syn"] and response["ack"] and not response["fin"] and
                            response["seq"] == self.seq_num + 1 and response["data"] == b""):
                            connected = True
                            break
                except socket.timeout:
                    debug("connect: timeout esperando SYN+ACK, reenvío SYN")
        finally:
            self.udp_socket.settimeout(previous_timeout)

        #la respuesta viene del nuevo socket del servidor, por lo que debemos actualizar 
        # la dirección de destino
        self.dest_address = server_address

        #tercer mensaje: ACK
        ack = {
            "ack": True,
            "syn": False,
            "fin": False,
            "seq": self.seq_num + 2,
            "data": b""
        }

        self._sendto(
            self.create_segment(ack),
            self.dest_address
        )

        #secuencia que se usara para enviar datos
        self.seq_num += 2

    def accept(self):
        #esperamos un SYN
        while True:
            syn_bytes, client_address = self.udp_socket.recvfrom(23)
            syn = self.parse_segment(syn_bytes)

            if (syn["syn"] and not syn["ack"] and not syn["fin"] and syn["data"] == b""):
                break

        #creamos un nuevo objeto para esta conexión
        connection_socketTCP = SocketTCP()

        #conservamos la IP local y pedimos otro puerto disponible
        local_ip = self.udp_socket.getsockname()[0]
        connection_socketTCP.bind((local_ip, 0))

        connection_socketTCP.dest_address = client_address
        new_address = connection_socketTCP.udp_socket.getsockname()

        #segundo mensaje: SYN+ACK desde el nuevo socket
        syn_ack = {
            "ack": True,
            "syn": True,
            "fin": False,
            "seq": syn["seq"] + 1,
            "data": b""
        }

        connection_socketTCP.udp_socket.settimeout(self.timeout)
        established = False
        while not established:
            connection_socketTCP._sendto(self.create_segment(syn_ack), client_address)
            try:
                while True:
                    ack_bytes, sender_address = connection_socketTCP.udp_socket.recvfrom(23)
                    ack = self.parse_segment(ack_bytes)
                    if sender_address != client_address:
                        continue
                    # llega el ACK final
                    if (ack["ack"] and not ack["syn"] and not ack["fin"]
                            and ack["seq"] == syn["seq"] + 2 and ack["data"] == b""):
                        established = True
                        break
                    # se perdió el ACK final y el cliente ya mandó datos, damos la conexión por establecida y NO lo ACKeamos
                    # el cliente lo retransmite y lo recibe recv()
                    if (not ack["ack"] and not ack["syn"] and not ack["fin"]
                            and ack["seq"] == syn["seq"] + 2):
                        established = True
                        break
            except socket.timeout:
                debug("accept: timeout esperando ACK final, reenvío SYN+ACK")

        connection_socketTCP.udp_socket.settimeout(None)   # vuelve a bloqueante
        connection_socketTCP.expected_seq_num = syn["seq"] + 2
        return connection_socketTCP, new_address

    def send(self, message, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            self.send_using_stop_and_wait(message)
        elif mode == "go_back_n":
            self.send_using_go_back_n(message)
        else:
            raise ValueError(f"modo desconocido: {mode}")

    def recv(self, buff_size, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            return self.recv_using_stop_and_wait(buff_size)
        elif mode == "go_back_n":
            return self.recv_using_go_back_n(buff_size)
        raise ValueError(f"modo desconocido: {mode}")

    def _gbn_send_slots(self, sock, data_window, first_index):
        # envía los elementos de la ventana desde first_index (el timer 0 parte solo si no estaba corriendo)
        for i in range(first_index, data_window.window_size):
            data = data_window.get_data(i)
            if data is None:
                break
            segment = self.create_segment({"ack": False, "syn": False, "fin": False,
                                           "seq": data_window.get_sequence_number(i),
                                           "data": data})
            sock.sendto(segment, self.dest_address, 0)

    def send_using_go_back_n(self, message):
        # primer trozo: largo del mensaje, luego el mensaje en trozos de chunk_size
        data_list = [len(message).to_bytes(4, byteorder="big")]
        for i in range(0, len(message), self.chunk_size):
            data_list.append(message[i:i + self.chunk_size])
        end_seq = self.seq_num + sum(len(d) for d in data_list)

        data_window = SlidingWindowCC(self.window_size, data_list, self.seq_num)

        # SocketUDP sobre NUESTRO socket UDP (mismo puerto) con pérdida simulada
        sock = SocketUDP()
        sock.socket_udp.close()
        sock.socket_udp = _LossySocket(self.udp_socket)
        sock.settimeout(self.timeout)

        previous_timeout = self.udp_socket.gettimeout()
        self.udp_socket.setblocking(False) # SocketUDP maneja los timers
        try:
            self._gbn_send_slots(sock, data_window, 0)

            while data_window.get_data(0) is not None:
                try:
                    ack_bytes, sender_address = sock.recvfrom(23)
                except TimeoutError:
                    # timeout, reenviamos toda la ventana
                    debug(f"GBN: timeout, reenvío ventana desde seq={data_window.get_sequence_number(0)}")
                    sock.stop_timer(0)
                    self._gbn_send_slots(sock, data_window, 0)
                    continue

                if sender_address != self.dest_address:
                    continue
                try:
                    ack = self.parse_segment(ack_bytes)
                except ValueError:
                    continue
                if not ack["ack"] or ack["syn"] or ack["fin"] or ack["data"] != b"":
                    continue

                # ACK acumulativo, cuántos elementos de la ventana confirma
                acked = 0
                for i in range(data_window.window_size):
                    data = data_window.get_data(i)
                    if data is None:
                        break
                    if data_window.get_sequence_number(i) + len(data) == ack["seq"]:
                        acked = i + 1
                        break
                if acked == 0:
                    continue  # ACK viejo/duplicado

                debug(f"GBN: ACK seq={ack['seq']}, confirma {acked} segmentos")
                data_window.move_window(acked)
                first_new = data_window.window_size - acked
                if data_window.get_data(0) is None:
                    break # todo confirmado
                if data_window.get_data(first_new) is not None:
                    # reiniciamos el timer y enviamos los nuevos
                    sock.stop_timer(0)
                    self._gbn_send_slots(sock, data_window, first_new)
        finally:
            if sock.timer_list[0] is not None:
                sock.stop_timer(0)
            self.udp_socket.settimeout(previous_timeout)

        self.seq_num = end_seq

    def send_using_stop_and_wait(self, message):
        message_len = len(message)

        #primero enviamos el largo del mensaje como un segmento de 4 bytes
        chunks = [message_len.to_bytes(4, byteorder="big")]

        # luego dividimos el mensaje en trozos de 16 bytes
        for i in range(0, message_len, 16):
            chunks.append(message[i:i + 16])

        # guardamos el timeout previo y establecemos el timeout para la espera de ACKs
        previous_timeout = self.udp_socket.gettimeout()
        self.udp_socket.settimeout(self.timeout)

        try:
            for chunk in chunks:
                #creamos el segmento con la secuencia actual
                segment = {
                    "ack": False,
                    "syn": False,
                    "fin": False,
                    "seq": self.seq_num,
                    "data": chunk
                }

                segment_bytes = self.create_segment(segment)
                confirmed = False

                while not confirmed:
                    #enviamos el segmento
                    self._sendto(segment_bytes, self.dest_address)

                    try:
                        while True:
                            #esperamos un ACK del mismo socket de destino
                            ack_bytes, sender_address = (self.udp_socket.recvfrom(23))

                            if sender_address != self.dest_address:
                                #si el ACK viene de otro socket, lo ignoramos
                                continue

                            #parseamos el ACK recibido
                            ack = self.parse_segment(ack_bytes)

                            if (ack["ack"] and not ack["syn"] and not ack["fin"] and 
                                ack["seq"] == self.seq_num + 1 and ack["data"] == b""):
                                #si el ACK es correcto, confirmamos y avanzamos la secuencia
                                confirmed = True
                                self.seq_num += 1
                                break

                    except socket.timeout:
                        #volvemos a enviar el mismo segmento
                        debug(f"send: timeout esperando ACK de seq={self.seq_num}, retransmito")
                        continue

        finally:
            self.udp_socket.settimeout(previous_timeout)

    def recv_using_stop_and_wait(self, buff_size):
        return self._recv(buff_size, bytes_seq=False)

    def recv_using_go_back_n(self, buff_size):
        # solo acepta el segmento con el seq esperado, si llega otro
        # duplicado o fuera de orden lo descarta y reenvia el último ACK
        return self._recv(buff_size, bytes_seq=True)

    def _recv(self, buff_size, bytes_seq):
        if buff_size < 0:
            raise ValueError("buff_size no puede ser negativo")

        if buff_size == 0:
            return b""

        if self.fin_seq is not None and self.remaining_bytes == 0:
            return b""

        waiting_length = self.remaining_bytes == 0

        if waiting_length:
            #si estamos esperando el tamaño del mensaje, no sabemos cuántos bytes recibir, 
            # así que no limitamos el tamaño
            tsize = None
        else:
            #el tamaño a recibir es el mínimo entre buff_size y los bytes restantes
            tsize = min(buff_size, self.remaining_bytes)

        message_received = b""

        while True:
            if not waiting_length:
                #si ya tenemos un tamaño definido, podemos recibir hasta tsize bytes
                chunk = self.recv_buffer[:tsize - len(message_received)]

                message_received += chunk
                self.recv_buffer = self.recv_buffer[len(chunk):]
                self.remaining_bytes -= len(chunk)

                if len(message_received) == tsize:
                    #si ya recibimos el tamaño esperado, retornamos el mensaje
                    return message_received

            segment_bytes, sender_address = self.udp_socket.recvfrom(23)

            if sender_address != self.dest_address:
                #si el segmento viene de otro socket, lo ignoramos
                continue

            segment = self.parse_segment(segment_bytes)

            if segment["fin"] and not segment["ack"] and not segment["syn"]:
                debug("recv: recibí FIN")
                self.fin_seq = segment["seq"]
                return message_received     # b"" si no había mensaje en curso

            if segment["ack"] or segment["syn"] or segment["fin"]:
                #si el segmento es un ACK, SYN o FIN, lo ignoramos
                continue

            if segment["seq"] == self.expected_seq_num:
                if waiting_length:
                    if len(segment["data"]) != 4:
                        #si el primer segmento no tiene 4 bytes, es un error
                        raise ValueError("Largo de mensaje inválido")

                    self.remaining_bytes = int.from_bytes(segment["data"], byteorder="big")
                    tsize = min(buff_size, self.remaining_bytes)
                    waiting_length = False

                else:
                    #si el segmento es el esperado, lo agregamos al buffer de recepción
                    self.recv_buffer += segment["data"]

                self.expected_seq_num += len(segment["data"]) if bytes_seq else 1
            else:
                debug(f"recv: duplicado seq={segment['seq']}, reenvío ACK")

            ack = {
                "ack": True,
                "syn": False,
                "fin": False,
                "seq": self.expected_seq_num,
                "data": b""
            }

            #enviamos el ACK correspondiente al segmento recibido
            self._sendto(self.create_segment(ack), self.dest_address)


    def _sendto(self, segment_bytes, address):
        if LOSS_RATE > 0 and random.random() < LOSS_RATE:
            debug(f"PERDIDO al enviar -> {self.parse_segment(segment_bytes)}")
            return
        self.udp_socket.sendto(segment_bytes, address)

    def close(self):
        """Host A: FIN -> espera FIN+ACK, hasta 3 timeouts -> ACK x3"""
        fin = {
            "ack": False,
            "syn": False,
            "fin": True,
            "seq": self.seq_num,
            "data": b""
        }
        self.udp_socket.settimeout(self.timeout)

        got_fin_ack = False
        for attempt in range(3):
            self._sendto(self.create_segment(fin), self.dest_address)
            try:
                while True:
                    seg_bytes, sender = self.udp_socket.recvfrom(23)
                    if sender != self.dest_address:
                        continue
                    seg = self.parse_segment(seg_bytes)
                    if (seg["fin"] and seg["ack"] and not seg["syn"]
                            and seg["seq"] == self.seq_num + 1):
                        got_fin_ack = True
                        break
            except socket.timeout:
                debug(f"close: timeout {attempt + 1}/3 esperando FIN+ACK")
                continue
            if got_fin_ack:
                break

        if got_fin_ack:
            ack = {
            "ack": True,
            "syn": False,
            "fin": False,
            "seq": self.seq_num + 2,
            "data": b""
        }
            for i in range(3):
                self._sendto(self.create_segment(ack), self.dest_address)
                if i < 2:
                    time.sleep(self.timeout)
        else:
            debug("close: 3 timeouts, la contraparte cerró")

        self.udp_socket.close()

    def recv_close(self):
        """Host B: (espera FIN) -> FIN+ACK -> espera ACK final, hasta 3 timeouts"""
        if self.fin_seq is None:
            self.udp_socket.settimeout(None)
            while self.fin_seq is None:
                seg_bytes, sender = self.udp_socket.recvfrom(23)
                if sender != self.dest_address:
                    continue
                seg = self.parse_segment(seg_bytes)
                if seg["fin"] and not seg["ack"] and not seg["syn"]:
                    self.fin_seq = seg["seq"]
                elif not (seg["ack"] or seg["syn"] or seg["fin"]):
                    # dato retransmitido porque se perdió nuestro último ACK
                    ack = {
                        "ack": True,
                        "syn": False,
                        "fin": False,
                        "seq": self.expected_seq_num,
                        "data": b""
                    }
                    self._sendto(self.create_segment(ack), self.dest_address)

        fin_ack = {
            "ack": True,
            "syn": False,
            "fin": True,
            "seq": self.fin_seq + 1,
            "data": b""
        }
        self.udp_socket.settimeout(self.timeout)

        for attempt in range(3):
            self._sendto(self.create_segment(fin_ack), self.dest_address)
            try:
                while True:
                    seg_bytes, sender = self.udp_socket.recvfrom(23)
                    if sender != self.dest_address:
                        continue
                    seg = self.parse_segment(seg_bytes)
                    if (seg["ack"] and not seg["syn"] and not seg["fin"]
                            and seg["seq"] == self.fin_seq + 2):
                        self.udp_socket.close()
                        return
            except socket.timeout:
                debug(f"recv_close: timeout {attempt + 1}/3 esperando ACK final")

        debug("recv_close: 3 timeouts, la contraparte cerró")
        self.udp_socket.close()


import socket


class UDPServer:
    def __init__(self, host="0.0.0.0", port=7777, buffer_size=1024):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))

        print(f"UDP Server listening on {self.host}:{self.port}")

    def receive(self) -> tuple[bytes, str]:
        data, addr = self.sock.recvfrom(self.buffer_size)

        ip, _ = addr
        try:
            hostname = socket.gethostbyaddr(ip)[0]
        except socket.herror:
            hostname = ""  # Couldnt reverse DNS

        return data, hostname

    def send(self, data: bytes, host: str, port=7777):
        try:
            self.sock.sendto(data, (host, port))
        except (socket.gaierror, OSError):
            pass

    def close(self):
        self.sock.close()

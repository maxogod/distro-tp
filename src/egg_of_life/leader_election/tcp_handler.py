from multiprocessing import Queue
import socket
import threading

CLOSE_SIGNAL = ("DONE", b"")


class TCPHandler:
    def __init__(self, host="0.0.0.0", port=6666, buffer_size=1024):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.is_running = threading.Event()
        self.is_running.set()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.connections: dict[str, socket.socket] = {}
        self.message_queue: Queue[tuple[str, bytes]] = Queue()

    def accept_connections(self):
        """Accept incoming connections (run in separate thread)"""
        self.sock.listen()
        while self.is_running.is_set():
            try:
                conn, addr = self.sock.accept()
                ip, port = addr
                hostname = socket.gethostbyaddr(ip)[0]

                print(f"Accepted from {hostname}:{port}")
                self.connections[hostname] = conn

                # Handle incoming messages from this connection
                threading.Thread(
                    target=self._handle_client, args=(conn, hostname), daemon=True
                ).start()
            except OSError:
                break

    def _handle_client(self, conn: socket.socket, hostname: str):
        """Handle messages from a single client"""
        try:
            while self.is_running.is_set():
                data = conn.recv(self.buffer_size)
                if not data:
                    break
                self.message_queue.put((hostname, data))
        except Exception as e:
            print(f"Error handling {hostname}: {e}")
        finally:
            conn.close()
            if hostname in self.connections:
                del self.connections[hostname]

    def receive(self) -> tuple[str, bytes]:
        """Receive data from any connected client (blocking)"""
        while self.is_running.is_set():
            hostname, data = self.message_queue.get()
            if (hostname, data) == CLOSE_SIGNAL:
                break
            return hostname, data

    def send(self, data: bytes, hostname: str):
        """Send data to a specific host"""

        print(f"Sending data to {hostname}")
        print(f"is {hostname} in connections? {hostname in self.connections}")

        self.connections[hostname].sendall(data)

    def connect_to(self, host: str, port: int):
        """Actively connect to another TCP server"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self.connections[host] = sock
            print(f"Connected to {host}:{port}")
            return sock
        except Exception as e:
            print(f"Failed to connect to {host}:{port}: {e}")
            return None

    def shutdown(self):
        """Close all connections and the server socket"""
        self.message_queue.join()
        self.is_running.clear()
        self.message_queue.put(CLOSE_SIGNAL)
        for conn in self.connections.values():
            conn.close()
        self.sock.close()

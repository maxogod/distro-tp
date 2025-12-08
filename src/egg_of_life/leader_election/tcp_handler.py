from queue import Queue
import socket
import threading
import time

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
        self.client_threads: list[threading.Thread] = []

    def normalize_hostname(self, hostname: str) -> str:
        return hostname.split(".")[0].split(":")[0]

    def accept_connections(self):
        """Accept incoming connections (run in separate thread)"""
        self.sock.listen()
        while self.is_running.is_set():
            try:
                conn, addr = self.sock.accept()
                ip, port = addr
                hostname = socket.gethostbyaddr(ip)[0]
                hostname = self.normalize_hostname(hostname)

                # print(f"Accepted from {hostname}:{port}")
                self.connections[hostname] = conn

                # Handle incoming messages from this connection
                thread = threading.Thread(
                    target=self._handle_client, args=(conn, hostname)
                )
                self.client_threads.append(thread)
                thread.start()
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
            self.message_queue.task_done()
            if (hostname, data) == CLOSE_SIGNAL:
                break
            return hostname, data
        return CLOSE_SIGNAL

    def send(self, data: bytes, hostname: str):
        """Send data to a specific host"""
        normalized_hostname = self.normalize_hostname(hostname)
        if normalized_hostname not in self.connections:
            print(f"Not connected to {normalized_hostname}, skipping send")
            return
        self.connections[normalized_hostname].sendall(data)

    def connect_to(self, host: str, port: int, retries: int = 5):
        """Actively connect to another TCP server"""
        for attempt in range(1, retries + 1):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                normalized_host = self.normalize_hostname(host)
                self.connections[normalized_host] = sock
                thread = threading.Thread(
                    target=self._handle_client, args=(sock, normalized_host)
                )
                self.client_threads.append(thread)
                thread.start()
                # print(f"Connected to {host}:{port}")
                return sock
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                    # print(f"Retrying connection to {host}:{port} (attempt {attempt})")

    def shutdown(self):
        """Close all connections and the server socket"""
        self.message_queue.join()
        self.is_running.clear()
        self.message_queue.put(CLOSE_SIGNAL)

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

        for conn in self.connections.values():
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()
            
        print("Joining client threads...")
        for thread in self.client_threads:
            thread.join()

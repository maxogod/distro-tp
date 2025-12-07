import socket
import threading


class TCPHandler:
    def __init__(self, host="0.0.0.0", port=6666, buffer_size=1024):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        print(f"TCP Server listening on {self.host}:{self.port}")
        self.connections: dict[str, socket.socket] = {}
        self.message_queue = []
        self.queue_lock = threading.Lock()
        self.queue_event = threading.Event()

    def accept_connections(self):
        """Accept incoming connections (run in separate thread)"""
        while True:
            try:
                conn, addr = self.sock.accept()
                ip, port = addr
                try:
                    hostname = socket.gethostbyaddr(ip)[0]
                except socket.herror:
                    hostname = ip

                print(f"Connected to {hostname}:{port}")
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
            while True:
                data = conn.recv(self.buffer_size)
                if not data:
                    break
                # Send data through channel (queue)
                with self.queue_lock:
                    self.message_queue.append((hostname, data))
                    self.queue_event.set()
        except Exception as e:
            print(f"Error handling {hostname}: {e}")
        finally:
            conn.close()
            if hostname in self.connections:
                del self.connections[hostname]

    def receive(self) -> tuple[str, bytes]:
        """Receive data from any connected client (blocking)"""
        while True:
            with self.queue_lock:
                if self.message_queue:
                    hostname, data = self.message_queue.pop(0)
                    if not self.message_queue:
                        self.queue_event.clear()
                    return hostname, data
            # Wait for new data
            self.queue_event.wait()

    def send(self, data: bytes, hostname: str) -> bool:
        """Send data to a specific host"""
        if hostname not in self.connections:
            return False
        try:
            self.connections[hostname].sendall(data)
            return True
        except Exception as e:
            print(f"Error sending to {hostname}: {e}")
            # Remove from connections if send failed
            with self.queue_lock:
                if hostname in self.connections:
                    del self.connections[hostname]
            return False

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

    def close(self):
        """Close all connections and the server socket"""
        for conn in self.connections.values():
            try:
                conn.close()
            except:
                pass
        self.sock.close()
        print("TCP Server closed")

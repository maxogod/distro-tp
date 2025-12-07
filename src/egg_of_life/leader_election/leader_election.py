import threading

from utils.logger import Logger
from .tcp_handler import TCPHandler
from protocol.leader_election_pb2 import (
    LeaderElection as LeaderElectionMsg,
)

ELECTION_MESSAGE = 1
CANDIDATE_MESSAGE = 2
ELECTION_TIMEOUT = 2  # seconds

HOSTNAME = "egg_of_life"


class LeaderElection:

    def __init__(
        self,
        amount_of_nodes: int,
        id: int,
        port=6666,
        sending_interval=200,
    ):
        self.id = id
        self.logger = Logger("LE")
        self.host_name = HOSTNAME
        self.sending_interval = sending_interval
        self.leader = 0
        self.amount_of_nodes = amount_of_nodes
        self.connected_nodes: dict[str, str] = dict()
        self.port = port
        self._server = TCPHandler(port=self.port)
        self.election_event = threading.Event()

    # ----------------- election listener -----------------

    def _election_listener(self) -> None:
        """Listen for incoming leader election messages"""
        while True:
            try:
                hostname, data = self._server.receive()
                msg = LeaderElectionMsg()
                msg.ParseFromString(data)

                if msg.message_type == ELECTION_MESSAGE:
                    self._handle_election_message(msg, hostname)
                elif msg.message_type == CANDIDATE_MESSAGE:
                    self._handle_candidate_message(msg, hostname)
                else:
                    raise ValueError(
                        f"Unknown message type {msg.message_type} from {hostname}"
                    )
            except Exception as e:
                self.logger.info(f"Error in election_listener: {e}")
                break

    # ----------------- election message handlers -----------------

    def _handle_election_message(self, msg: LeaderElectionMsg, hostname: str) -> None:
        sender_id = int(msg.id)
        self.logger.info(f"Received ELECTION from {hostname} with id={sender_id}")
        # reply to the sender
        self._send_candidate_message(hostname)
        # if I have higher ID, I should start my own election
        if self.id > sender_id:
            self.logger.info(
                f"Node {self.id}: I am higher than {sender_id}, starting my own election"
            )
            self.start_election()

    def _handle_candidate_message(self, msg: LeaderElectionMsg, hostname: str) -> None:
        sender_id = int(msg.id)
        self.logger.info(f"Received CANDIDATE from {hostname} with id={msg.id}")
        if sender_id > self.leader and sender_id > self.id:
            self.leader = msg.id
            self.election_event.set()
            self.logger.info(f"Node {self.id}: New leader is {self.leader}")

    def _election_timeout_handler(self) -> None:
        """Handle election timeout - if no higher node responds, assume leadership"""
        if not self.election_event.wait(timeout=ELECTION_TIMEOUT):
            # Timeout occurred - no higher node responded
            self.logger.info(f"Node {self.id}: Election timeout - assuming leadership")
            self.leader = self.id
            # Broadcast that we are the leader
            self._send_candidate_message()

    # ----------------- message senders -----------------

    def _send_election_messages(self) -> None:
        """Send election messages to all nodes with higher IDs"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = ELECTION_MESSAGE

        for node in self.connected_nodes.values():
            node_id = self._get_connection_id(node)
            if node_id > self.id:
                self.logger.info(f"Sending ELECTION to {node}")
                self._server.send(msg.SerializeToString(), node)

    def _send_candidate_message(self, target=None) -> None:
        """Send candidate message to specific node or all nodes"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = CANDIDATE_MESSAGE

        if target:
            self.logger.info(f"Replying to {target} with CANDIDATE")
            self._server.send(msg.SerializeToString(), target)
            return

        for node in self.connected_nodes.values():
            self.logger.info(f"Sending CANDIDATE to {node}")
            self._server.send(msg.SerializeToString(), node)

    # ----------------- helper methods -----------------

    def _get_hostname(self, raw: str) -> str:
        """Convert raw TCP name (may include domain or port) to canonical node name."""
        raw = raw.split(":")[0]  # strip port
        raw = raw.split(".")[0]  # strip domain
        return raw

    def _get_connection_id(self, hostname: str) -> int:
        """Get connection by hostname"""
        hostname = self._get_hostname(hostname)
        return int(hostname.split(HOSTNAME)[-1])

    # ----------------- public methods -----------------

    def start(self):
        self.logger.info(f"Node {self.id} is starting on port {self.port}")

        # Start accepting incoming TCP connections
        self.accept_thread = threading.Thread(
            target=self._server.accept_connections, daemon=True
        )
        self.accept_thread.start()

        # Actively connect to other nodes
        for node_id in range(1, self.amount_of_nodes + 1):
            node = f"{self.host_name}{node_id}"
            if node_id != self.id:
                self._server.connect_to(node, self.port)

        # Wait until all expected peers are connected
        while True:
            connections = set(self._server.connections.keys())
            if len(connections) == self.amount_of_nodes - 1:
                for conn in connections:
                    hostname = self._get_hostname(conn)
                    self.connected_nodes[hostname] = conn
                break
        self.logger.info(f"Node {self.id}: all peers connected: {self.connected_nodes.values()}")

        # Start message listener
        self.listener_thread = threading.Thread(
            target=self._election_listener, daemon=True
        )
        self.listener_thread.start()

        self.start_election()

    def start_election(self) -> None:
        """Start the election process"""
        self.logger.info(f"Node {self.id} starting election")
        self.election_event.clear()
        self._send_election_messages()
        # Start timeout handler
        threading.Thread(target=self._election_timeout_handler, daemon=True).start()

    def i_am_leader(self) -> bool:
        return self.leader and self.leader == self.id

    def get_nodes(self) -> list[str]:
        return list(self.connected_nodes.keys())

    def get_leader(self) -> str:
        return f"{self.host_name}{self.leader}" if self.leader else None

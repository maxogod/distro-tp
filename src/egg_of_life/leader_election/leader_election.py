import threading
from typing import Optional

from utils.logger import Logger
from .tcp_handler import TCPHandler
from protocol.leader_election_pb2 import LeaderElection as LeaderElectionMsg

ELECTION_MESSAGE = 1
COORDINATOR_MESSAGE = 2
CANDIDATE_MESSAGE = 3
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
        self.connected_nodes: set[str] = set()
        self.port = port
        self._server = TCPHandler(port=self.port)
        self.election_event = threading.Event()
        self.coordinator_event = threading.Event()

    # ----------------- election listener -----------------

    def _election_listener(self) -> None:
        """Listen for incoming leader election messages"""
        while True:
            try:
                hostname, data = self._server.receive()
                msg = LeaderElectionMsg()
                msg.ParseFromString(data)
                self.logger.info(
                    f"Node {self.id} received message type {msg.message_type} from {hostname}"
                )

                if msg.message_type == ELECTION_MESSAGE:
                    self._handle_election_message(msg, hostname)
                elif msg.message_type == CANDIDATE_MESSAGE:
                    self._handle_candidate_message(msg, hostname)
                elif msg.message_type == COORDINATOR_MESSAGE:
                    self._handle_coordinator_message(msg, hostname)
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
        self.logger.info(f"Received CANDIDATE from {hostname} with id={msg.id}")
        self.election_event.set()

    def _handle_coordinator_message(
        self, msg: LeaderElectionMsg, hostname: str
    ) -> None:
        self.logger.info(f"Received COORDINATOR from {hostname} with id={msg.id}")
        self.coordinator_event.set()
        self.election_event.set()
        self.leader = msg.id

    def _election_timeout_handler(self) -> None:
        """Handle election timeout - if no higher node responds, assume leadership"""
        if not self.election_event.wait(timeout=ELECTION_TIMEOUT):
            # Timeout occurred - no higher node responded
            if not self.coordinator_event.is_set():
                self.logger.info(f"Node {self.id}: Election timeout - assuming leadership")
                self.leader = self.id
                # Broadcast that we are the leader
                self._send_coordinator_message()

    def _coordinator_timeout_handler(self) -> None:
        """Handle coordinator timeout - if no coordinator message is received, start a new election"""
        if not self.coordinator_event.wait(timeout=ELECTION_TIMEOUT):
            self.logger.info(
                f"Node {self.id}: Coordinator timeout - starting new election"
            )
            self.start_election()

    # ----------------- message senders -----------------

    def _send_election_messages(self) -> None:
        """Send election messages to all nodes with higher IDs"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = ELECTION_MESSAGE

        for node in self.connected_nodes:
            node_id = self._get_connection_id(node)
            if node_id > self.id:
                self.logger.info(f"Sending ELECTION to {node}")
                self._server.send(msg.SerializeToString(), node)

    def _send_candidate_message(self, target) -> None:
        """Send candidate message to specific node or all nodes"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = CANDIDATE_MESSAGE

        self.logger.info(f"Replying to {target} with CANDIDATE")
        self._server.send(msg.SerializeToString(), target)
        return

    def _send_coordinator_message(self) -> None:
        """Send coordinator message to all nodes"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = COORDINATOR_MESSAGE

        for node in self.connected_nodes:
            self.logger.info(f"Sending COORDINATOR to {node}")
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
        threading.Thread(target=self._server.accept_connections, daemon=True).start()

        # Actively connect to other nodes
        for node_id in range(1, self.amount_of_nodes + 1):
            node = f"{self.host_name}{node_id}"
            if node_id != self.id:
                self._server.connect_to(node, self.port)

        # Wait until all expected peers are connected
        self._server.wait_for_connections(self.amount_of_nodes - 1)
        self.connected_nodes = set(self._server.connections.keys())
        self.logger.info(
            f"Node {self.id}: all peers connected: {list(self.connected_nodes)}"
        )

        # Start message listener
        threading.Thread(target=self._election_listener, daemon=True).start()

        self.start_election()

    def start_election(self) -> None:
        """Start the election process"""
        self.logger.info(f"Node {self.id} starting election")
        self.election_event.clear()
        self._send_election_messages()
        # Start timeout handler
        threading.Thread(target=self._election_timeout_handler, daemon=True).start()

    def i_am_leader(self) -> bool:
        return self.leader != 0 and self.leader == self.id

    def get_nodes(self) -> list[str]:
        return list(self.connected_nodes)

    def get_leader(self) -> Optional[str]:
        return f"{self.host_name}{self.leader}" if self.leader else None

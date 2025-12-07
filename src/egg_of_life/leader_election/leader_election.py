import threading
import time
from egg_of_life.leader_election.tcp_handler import TCPHandler
from egg_of_life.leader_election.leader_election_pb2 import (
    LeaderElection as LeaderElectionMsg,
)

ELECTION_MESSAGE = 1
CANDIDATE_MESSAGE = 2
ELECTION_TIMEOUT = 5  # seconds


class LeaderElection:
    def __init__(
        self, amount_of_nodes: int, id: int, host="eol", port=6666, sending_interval=200
    ):
        self.id = id
        self.host_name = host
        self.sending_interval = sending_interval
        self.leader = None
        self.nodes = [
            f"{host}{i}" for i in range(1, amount_of_nodes + 1) if i != self.id
        ]
        self.port = port
        self._server = TCPHandler(port=self.port, host=host)
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
                    self._handle_election_message(hostname)
                elif msg.message_type == CANDIDATE_MESSAGE:
                    self._handle_candidate_message(msg, hostname)
                else:
                    raise ValueError(
                        f"Unknown message type {msg.message_type} from {hostname}"
                    )
            except Exception as e:
                print(f"Error in election_listener: {e}")
                break

    # ----------------- election message handlers -----------------

    def _handle_election_message(self, hostname: str) -> None:
        print(f"Received ELECTION from {hostname}")
        self.election_event.clear()
        self._send_candidate_message(hostname)
        # Start timeout to assume leadership if no higher node responds
        self._send_election_messages()
        threading.Thread(target=self._election_timeout_handler, daemon=True).start()

    def _handle_candidate_message(self, msg: LeaderElectionMsg, hostname: str) -> None:
        print(f"Received CANDIDATE from {hostname} with id={msg.id}")
        if msg.id > self.leader or self.leader is None:
            self.leader = msg.id
            self.election_event.set()

    def _election_timeout_handler(self) -> None:
        """Handle election timeout - if no higher node responds, assume leadership"""
        if not self.election_event.wait(timeout=ELECTION_TIMEOUT):
            # Timeout occurred - no higher node responded
            print(f"Node {self.id}: Election timeout - assuming leadership")
            self.leader = self.id
            # Broadcast that we are the leader
            self._send_candidate_message()

    # ----------------- message senders -----------------

    def _send_election_messages(self) -> None:
        """Send election messages to all nodes with higher IDs"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = ELECTION_MESSAGE

        for node in self.nodes:
            node_id = int(node.replace("eol", ""))
            if node_id > self.id:
                print(f"Sending ELECTION to {node}")
                self._server.send(msg.SerializeToString(), node)

    def _send_candidate_message(self, target_node: str = None) -> None:
        """Send candidate message to specific node or all nodes"""
        msg = LeaderElectionMsg()
        msg.id = self.id
        msg.message_type = CANDIDATE_MESSAGE

        if target_node:
            print(f"Sending CANDIDATE to {target_node}")
            self._server.send(msg.SerializeToString(), target_node)
        else:
            for node in self.nodes:
                if node != f"eol{self.id}":
                    print(f"Sending CANDIDATE to {node}")
                    self._server.send(msg.SerializeToString(), node)

    # ----------------- public methods -----------------

    def start(self) -> None:
        """Start the election process"""
        self.listener_thread = threading.Thread(
            target=self._election_listener, daemon=True
        )
        self.listener_thread.start()

    def start_election(self) -> None:
        """Start the election process"""
        print(f"Node {self.id} starting election")
        self.election_event.clear()
        self._send_election_messages()
        # Start timeout handler
        threading.Thread(target=self._election_timeout_handler, daemon=True).start()

    def i_am_leader(self) -> bool:
        return self.leader and self.leader == self.id

    def get_nodes(self) -> list[str]:
        return self.nodes

    def get_leader(self) -> str:
        return f"{self.host_name}{self.leader}" if self.leader else None

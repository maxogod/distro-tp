import os
import yaml


class Config:
    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        
        # YAML config
        self.port = cfg.get("port", 7777)
        self.timeout_interval = cfg.get("timeout_interval", 10)
        self.check_interval = cfg.get("check_interval", 5)
        self.leader_election_port = cfg.get("leader_election", {}).get("port", 6666)
        self.heartbeat_interval = cfg.get("leader_election", {}).get("interval", 200)
        
        # Environment variables
        self.docker_network = os.getenv("NETWORK", "bridge")
        self.host_path = os.getenv("HOST_PROJECT_PATH", "")
        self.controller_count = int(os.getenv("MAX_CONTROLLER_NODES", "0"))
        self.amount_of_nodes = int(os.getenv("AMOUNT_OF_NODES", "1"))
        
        id_str = os.getenv("ID", "")
        if not id_str:
            raise ValueError("ID environment variable not set")
        self.id = int(id_str)
        self.name = f"egg_of_life{self.id}"

    def __str__(self):
        return (
            f"Revival Chansey Config:\n"
            f"  Port: {self.port}, Timeout: {self.timeout_interval}s, Check: {self.check_interval}s\n"
            f"  Docker: {self.docker_network}, Host: {self.host_path}, Controllers: {self.controller_count}\n"
            f"  Leader Election: port={self.leader_election_port}, interval={self.heartbeat_interval}ms\n"
            f"  Nodes: {self.amount_of_nodes}, ID: {self.id}"
        )

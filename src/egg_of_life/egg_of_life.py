#!/bin/python3

import threading
import re
import signal
import time
import yaml
import os
from leader_election.leader_election import LeaderElection
from udp_server import UDPServer
from docker_runner import DockerRunner
from protocol.heartbeat_pb2 import HeartBeat
from google.protobuf.message import DecodeError
from utils.config import Config

# Config
CONFIG_PATH = "/app/config.yaml"


class RevivalChansey:
    def __init__(
        self,
        timeout_interval: int,
        check_interval: int,
        docker_network: str,
        host_path: str,
        amount_of_nodes: int,
        controller_count: int,
        leader_election: LeaderElection,
        heartbeatConn: UDPServer,
    ):
        self._server = heartbeatConn
        self._docker_runner = DockerRunner(
            docker_network, host_path, controller_count, amount_of_nodes
        )

        self._timeout_interval = timeout_interval
        self._check_interval = check_interval

        self.running = threading.Event()
        self.running.set()

        # Track last heartbeat timestamps
        self._last_heartbeat: dict[str, float] = {}
        self._heartbeat_lock = threading.Lock()
        self.revive_set = set()

        # Leader Election
        self._leader_election = leader_election

    def start(self):
        print("Revival Chansey ready")
        self._monitor_thread = threading.Thread(
            target=self._monitor_timeouts, daemon=True
        )
        self._heartbeat_thread = threading.Thread(
            target=self._send_heartbeats, daemon=True
        )
        self._monitor_thread.start()
        self._docker_runner.start()
        self._heartbeat_thread.start()  # we start hb sending thread
        self._leader_election.start()  # start leader election

        while self.running.is_set():
            try:
                hostname = self._get_heartbeat()
                if hostname:
                    nodename = hostname.split(".")[0]
                    self._update_timestamp(nodename)
                    if nodename in self.revive_set:
                        print(f"Node {nodename} revived")
                        self.revive_set.remove(nodename)
            except OSError:
                print(f"Socket closed, shutting down")
                self.shutdown()

    def shutdown(self):
        if not self.running.is_set():
            return
        self.running.clear()  # Sets flag to false
        self._server.close()
        self._monitor_thread.join()
        self._heartbeat_thread.join()
        self._docker_runner.shutdown()
        self._leader_election.shutdown()
        print("Revival Chansey finished")

    def _send_heartbeats(self):
        connectedNodes = self._leader_election.get_nodes()

        while True:
            if not self._leader_election.i_am_leader():  # only the leader sends hb
                time.sleep(0.1)
                continue
            hb = HeartBeat()
            data = hb.SerializeToString()
            for node in connectedNodes:
                try:
                    self._server.send(
                        data,
                        node,
                    )  # UDP send, node is hostname, data is bytes
                except Exception as e:
                    print(f"Could not send heartbeat to {node}: {e}")
                    pass  # Silently ignore if nodes not reachable yet
            time.sleep(self._leader_election.sending_interval / 1000)

    def _get_heartbeat(self) -> str:
        data, hostname = self._server.receive()
        hb = HeartBeat()

        try:
            hb.ParseFromString(data)
        except DecodeError:
            print(f"Received invalid heartbeat data {data}")
            return ""

        return hostname

    def _get_image_name(self, container_name) -> str:
        return re.sub(r"\d+$", "", container_name) + ":latest"

    def _update_timestamp(self, hostname):
        with self._heartbeat_lock:
            if not hostname in self._last_heartbeat:
                print(f"Registering {hostname}")
            self._last_heartbeat[hostname] = time.time()

    def _on_timeout(self, hostname):
        print(f"Timeout detected for {hostname}")

        if hostname == self._leader_election.get_leader():
            self._leader_election.start_election()

        # Wait for election to resolve
        self._leader_election.wait_for_election_resolution()

        if not self._leader_election.i_am_leader():
            print(f"Not the leader, skipping revival of {hostname}")
            self.revive_set.add(hostname)
            return

        # Cleanup and restart
        print(f"Reviving node {hostname}")
        image = self._get_image_name(hostname)
        self._docker_runner.cleanup_container(hostname)
        self._docker_runner.restart_container(hostname, image)
        self.revive_remaining_nodes()

    def _monitor_timeouts(self):
        """Monitor heartbeats and handle timeouts on another Thread"""
        while self.running.is_set():
            current_time = time.time()
            timed_out = []

            with self._heartbeat_lock:
                for hostname, last_time in list(self._last_heartbeat.items()):
                    if current_time - last_time > self._timeout_interval:
                        timed_out.append(hostname)
                        del self._last_heartbeat[hostname]

            for hostname in timed_out:
                self._on_timeout(hostname)

            time.sleep(self._check_interval)

    def revive_remaining_nodes(self):
        for node in self.revive_set:
            print(f"Also reviving previously timed out node {node}")
            image = self._get_image_name(node)
            self._docker_runner.cleanup_container(node)
            self._docker_runner.restart_container(node, image)
        self.revive_set.clear()


def setup_signal_handlers(rc: RevivalChansey):
    def signal_handler(_sig, _frame):
        rc.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    # Requires protobufs to exist in ./protocol/
    # protoc --python_out=./src/egg_of_life ./src/common/protobufs/protocol/heartbeat.proto
    # protoc --python_out=./src/egg_of_life ./src/common/protobufs/protocol/leader_election.proto

    config = Config(CONFIG_PATH)

    heartbeatConn = UDPServer(port=config.port)

    print(config)

    le = LeaderElection(
        id=config.id,
        amount_of_nodes=config.amount_of_nodes,
        port=config.leader_election_port,
        sending_interval=config.heartbeat_interval,
        heartbeatConn=heartbeatConn,
    )

    rc = RevivalChansey(
        config.timeout_interval,
        config.check_interval,
        config.docker_network,
        config.host_path,
        config.amount_of_nodes,
        config.controller_count,
        le,
        heartbeatConn,
    )

    setup_signal_handlers(rc)

    rc.start()


if __name__ == "__main__":
    main()

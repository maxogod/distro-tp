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
        config: Config,
        leader_election: LeaderElection,
        heartbeatConn: UDPServer,
    ):
        self._server = heartbeatConn
        self._docker_runner = DockerRunner(config)

        self._timeout_interval = config.timeout_interval
        self._check_interval = config.check_interval

        self.running = threading.Event()
        self.running.set()

        # Track last heartbeat timestamps
        self._last_heartbeat: dict[str, float] = {}
        self.revive_set = set()
        self._state_lock = threading.Lock()

        # Leader Election
        self._leader_election = leader_election
        self._leader_election.set_on_became_leader_callback(self._on_became_leader)

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
            except OSError:
                self.shutdown()

    def shutdown(self):
        if not self.running.is_set():
            return
        self.running.clear()  # Sets flag to false
        self._server.close()
        self._monitor_thread.join()
        self._heartbeat_thread.join()
        self._docker_runner.shutdown(self._leader_election.i_am_leader())
        self._leader_election.shutdown()
        print("Revival Chansey finished")

    def _send_heartbeats(self):
        print("Starting to send heartbeats to peers")

        while self.running.is_set():
            hb = HeartBeat()
            data = hb.SerializeToString()
            for node in self._leader_election.get_nodes():
                try:
                    self._server.send(data, node)
                except Exception as e:
                    print(f"Error sending heartbeat to {node}: {e}")
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
        with self._state_lock:
            if hostname in self.revive_set:
                print(f"Node {hostname} revived")
                self.revive_set.remove(hostname)
            if not hostname in self._last_heartbeat:
                print(f"Registering {hostname}")
            self._last_heartbeat[hostname] = time.time()

    def _on_timeout(self, hostname):
        print(f"Timeout detected for {hostname}")

        if hostname == self._leader_election.get_leader():
            self._leader_election.start_election()

        with self._state_lock:
            if not self._leader_election.i_am_leader():
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

            with self._state_lock:
                for hostname, last_time in list(self._last_heartbeat.items()):
                    if current_time - last_time > self._timeout_interval:
                        timed_out.append(hostname)
                        del self._last_heartbeat[hostname]

            for hostname in timed_out:
                self._on_timeout(hostname)

            time.sleep(self._check_interval)

    def revive_remaining_nodes(self):
        eols = []
        with self._state_lock:
            for node in list(self.revive_set):
                if "egg_of_life" in node:
                    eols.append(node)
                    continue
                print(f"Reviving previously timed out node {node}")
                image = self._get_image_name(node)
                self._docker_runner.cleanup_container(node)
                self._docker_runner.restart_container(node, image)
            self.revive_set.clear()
        for eol in eols:
            print(f"Reviving previously timed out Egg of Life node {eol}")
            image = self._get_image_name(eol)
            self._docker_runner.cleanup_container(eol)
            self._docker_runner.restart_container(eol, image)

    def _on_became_leader(self, old_leader=None):
        self.revive_remaining_nodes()


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
        config,
        le,
        heartbeatConn,
    )

    setup_signal_handlers(rc)

    rc.start()


if __name__ == "__main__":
    main()

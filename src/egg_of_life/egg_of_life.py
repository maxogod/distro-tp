#!/bin/python3

import threading
import re
import signal
import time
import yaml
import os
from udp_server import UDPServer
from docker_runner import DockerRunner
from protocol.heartbeat_pb2 import HeartBeat
from google.protobuf.message import DecodeError

# Config
CONFIG_PATH="/app/config.yaml"


class RevivalChansey:
    def __init__(self, port: int, timeout_interval: int, check_interval: int, docker_network: str, host_path: str, controller_count: int):
        self._server = UDPServer(port=port)
        self._docker_runner = DockerRunner(docker_network, host_path, controller_count)

        self._timeout_interval = timeout_interval
        self._check_interval = check_interval

        self.running = threading.Event()
        self.running.set()

        # Track last heartbeat timestamps
        self._last_heartbeat: dict[str, float] = {}
        self._heartbeat_lock = threading.Lock()

    def start(self):
        print("Revival Chansey ready")
        self._monitor_thread = threading.Thread(target=self._monitor_timeouts, daemon=True)
        self._monitor_thread.start()
        self._docker_runner.start()

        while self.running.is_set():
            try:
                hostname = self._get_heartbeat()
                if hostname:
                    nodename = hostname.split(".")[0]
                    self._update_timestamp(nodename)
            except OSError:
                print(f"Socket closed, shutting down")
                self.shutdown()

    def shutdown(self):
        if not self.running.is_set(): return
        self.running.clear() # Sets flag to false
        self._server.close()
        self._monitor_thread.join()
        self._docker_runner.shutdown()
        print("Revival Chansey finished")

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
        return re.sub(r'\d+$', '', container_name) + ":latest"

    def _update_timestamp(self, hostname):
        with self._heartbeat_lock:
            if not hostname in self._last_heartbeat:
                print(f"Registering {hostname}")
            self._last_heartbeat[hostname] = time.time()

    def _on_timeout(self, hostname):
        print(f"Timeout detected for {hostname}")
        
        # Cleanup and restart
        image = self._get_image_name(hostname)
        self._docker_runner.cleanup_container(hostname)
        self._docker_runner.restart_container(hostname, image)

    def _monitor_timeouts(self):
        """ Monitor heartbeats and handle timeouts on another Thread """
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

def setup_signal_handlers(rc: RevivalChansey):
    def signal_handler(_sig, _frame):
        rc.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    # Requires protobuf to exist in ./protocol/heartbeat_pb2.py
    # protoc --python_out=./src/egg_of_life ./src/common/protobufs/protocol/heartbeat.proto

    port = 7777
    timeout_interval = 10
    check_interval = 5
    docker_network = os.getenv("NETWORK", "bridge")
    host_path = os.getenv("HOST_PROJECT_PATH", "")
    controller_count = int(os.getenv("MAX_CONTROLLER_NODES", "0"))
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
            cfg.get("port", port)
            cfg.get("timeout_interval", timeout_interval)
            cfg.get("check_interval", check_interval)

    print(f"Starting Revival Chansey on port {port} with timeout {timeout_interval}s and check interval {check_interval}s")
    print(f"Docker network: {docker_network}, Host path: {host_path}, Max controllers: {controller_count}")

    rc = RevivalChansey(port, timeout_interval, check_interval, docker_network, host_path, controller_count)

    setup_signal_handlers(rc)

    rc.start()


if __name__ == "__main__":
    main()

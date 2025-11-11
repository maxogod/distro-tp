#!/bin/python3

import subprocess
import threading
import re
import signal
import time
import sys
import yaml
import os
from udp_server import UDPServer
from protocol.heartbeat_pb2 import HeartBeat
from google.protobuf.message import DecodeError

# Config
CREATOR_LABEL = "revived_by=revival_chansey"
CONFIG_PATH="/app/config.yaml"

def run_cmd(cmd_str):
    arg_list = cmd_str.split(" ")
    res = subprocess.run(
            arg_list,
            capture_output=True,
            text=True,
        )
    if res.stderr and res.stderr.strip():
        print(res.stderr.strip())
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd_str}\n{res.stderr}")
    return res.stdout.strip()


class RevivalChansey:
    def __init__(self, port: int, timeout_interval: int, check_interval: int, docker_network: str):
        self._server = UDPServer(port=port)
        self._network = docker_network
        self._timeout_interval = timeout_interval
        self._check_interval = check_interval

        self.running = threading.Event()
        self.running.set()

        # Track last heartbeat timestamps
        self._last_heartbeat: dict[str, float] = {}
        self._heartbeat_lock = threading.Lock()

        self._monitor_thread = threading.Thread(target=self._monitor_timeouts, daemon=True)
        self._monitor_thread.start()

    def start(self):
        print("Revival Chansey ready")

        while self.running.is_set():
            hostname = self._get_heartbeat()
            if hostname:
                nodename = hostname.split(".")[0]
                self._update_timestamp(nodename)

    def shutdown(self):
        self.running.clear() # Sets flag to false
        self._server.close()
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

    def _restart_container(self, name, image):
        print(f"Launching new container {name} (image: {image}) on network {self._network}")

        cmd = (
            f"docker run -d --name {name} "
            f"--network {self._network} "
            f"--label {CREATOR_LABEL} "
            f"{image}"
        )
        try:
            out = run_cmd(cmd)
            print(f"New container {name} created (ID: {out.strip()})")
        except:
            print(f"Error creating container {name}")

    def _cleanup_container(self, container_name):
        try:
            run_cmd(f"docker stop {container_name}")
            run_cmd(f"docker rm {container_name}")
        except:
            pass

    def _update_timestamp(self, hostname):
        with self._heartbeat_lock:
            if not hostname in self._last_heartbeat:
                print(f"Registering {hostname}")
            self._last_heartbeat[hostname] = time.time()

    def _on_timeout(self, hostname):
        print(f"Timeout detected for {hostname}")
        
        # Cleanup and restart
        image = self._get_image_name(hostname)
        self._cleanup_container(hostname)
        self._restart_container(hostname, image)

    def _monitor_timeouts(self):
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
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    # Requires protobuf to exist in ./protocol/heartbeat_pb2.py
    # protoc --python_out=./src/egg_of_life ./src/common/protobufs/protocol/heartbeat.proto

    port = 7777
    timeout_interval = 10
    check_interval = 5
    docker_network = os.getenv("NETWORK") or "bridge"
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
            cfg.get("port", port)
            cfg.get("timeout_interval", timeout_interval)
            cfg.get("check_interval", check_interval)

    rc = RevivalChansey(port, timeout_interval, check_interval, docker_network)

    setup_signal_handlers(rc)

    rc.start()


if __name__ == "__main__":
    main()

#!/bin/python3

import requests
import time
import subprocess
import random

HEALTHY = 200
RABBITMQ_CONTAINER = "rabbitmq"

def run_cmd(cmd_str):
    arg_list = cmd_str.split(" ")
    res = subprocess.run(
            arg_list,
            capture_output=True,
            text=True,
        )
    if res.stderr != "":
        print(res.stderr)
    return res.stdout

class HoundDoom:
    def __init__(self, network, prefix="", exeption_prefix="None"):
        self.network = network
        self.prefix = prefix
        self.exeption_prefix = exeption_prefix
        self.containers = self._get_container_names()

    def unleash_doom(self):
        if len(self.containers) == 0:
            print("No doom if no containers are up :(")
            return

        indexes = list(range(len(self.containers)))
        random.shuffle(indexes)

        for i in indexes:
            container = self.containers[i]
            has_prefix = container.startswith(self.prefix)
            has_exeption = container.startswith(self.exeption_prefix)
            if container == "" or not has_prefix or has_exeption or container == RABBITMQ_CONTAINER:
                continue
            self._kill(container, 8081)
            time.sleep(3)

    def _get_container_names(self):
        res = run_cmd("docker ps --format '{{.Names}}'")
        return res.split("\n")

    def _kill(self, container_name, healthcheck_port):
        if not self._is_healthy(container_name, healthcheck_port):
            return

        print(f"Killing {container_name}")
        run_cmd(f"docker kill {container_name}")

    def _is_healthy(self, container_name, healthcheck_port):
        return True
        # TODO: handle networking to be able to hit these endpoints
        url = f"http://{container_name}:{healthcheck_port}"
        try:
            res = requests.get(url)
            if res.status_code != HEALTHY:
                print(f"Container {container_name} is already unhealthy")
                return False
        except:
            print(f"Container {container_name} is already dead")
            return False
        return True

def main():
    kong = HoundDoom("atus")
    kong.unleash_doom()

if __name__ == "__main__":
    main()

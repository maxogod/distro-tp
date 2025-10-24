#!/bin/python3

import time
import subprocess
import random

HEALTHY = 200
CONTAINER_NAME_POS = 0
CONTAINER_ID_POS = 1
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
    def __init__(self, amount_to_doom, prefix="", exeption_prefix="None"):
        self.amount_to_doom = amount_to_doom
        self.prefix = prefix
        self.exeption_prefix = exeption_prefix
        containers = self._get_container_names()
        self.containers = [value.strip("'").split(",") for value in containers]
        self.containers.pop(-1) # remove last empty string

    def unleash_doom(self):
        if len(self.containers) == 0:
            print("No doom if no containers are up :(")
            return

        indexes = list(range(len(self.containers)))
        random.shuffle(indexes)
        indexes = indexes[:self.amount_to_doom]

        for i in indexes:
            container_name = self.containers[i][CONTAINER_NAME_POS]
            has_prefix = container_name.startswith(self.prefix)
            has_exeption = container_name.startswith(self.exeption_prefix)
            if container_name == "" or not has_prefix or has_exeption or container_name == RABBITMQ_CONTAINER:
                print(f"Skipping {container_name}")
                continue
            container_id = self.containers[i][CONTAINER_ID_POS]
            self._kill(container_name, container_id)
            time.sleep(3)

    def _get_container_names(self):
        res = run_cmd("docker ps --format '{{.Names}},{{.ID}}'")
        return res.split("\n")

    def _kill(self, container_name, container_id):
        print(f"Killing {container_name}")
        run_cmd(f"docker kill {container_id}")

def main():
    hound = HoundDoom(3)
    hound.unleash_doom()

if __name__ == "__main__":
    main()

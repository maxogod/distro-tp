#!/bin/python3

from sys import argv
import time
import subprocess
import random

CONTAINER_NAME_POS = 0
CONTAINER_ID_POS = 1
DEFAULT_EXEPTIONS = "none"
RABBITMQ_CONTAINER = "rabbitmq"
EGG_OF_LIFE_CONTAINER = "egg_of_life"

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
    def __init__(self, amount_to_doom, time_wait, prefix="", exeption_prefix=DEFAULT_EXEPTIONS):
        self.amount_to_doom = amount_to_doom
        self.time_wait = time_wait
        self.prefix = prefix
        self.exeption_prefix = exeption_prefix
        containers = self._get_container_names()
        self.containers = [value.strip("'").split(",") for value in containers]
        self.containers.pop(-1) # remove last empty string

    def unleash_doom(self):
        eligible_containers = [
            container for container in self.containers
            if self._is_eligible_for_doom(container[CONTAINER_NAME_POS])
        ]
        
        if len(eligible_containers) == 0:
            print("No doom if no eligible containers are up :(")
            return

        num_to_doom = min(self.amount_to_doom, len(eligible_containers))
        containers_to_doom = random.sample(eligible_containers, num_to_doom)

        for container in containers_to_doom:
            container_name = container[CONTAINER_NAME_POS]
            container_id = container[CONTAINER_ID_POS]
            self._kill(container_name, container_id)
            time.sleep(self.time_wait)

    def _get_container_names(self):
        res = run_cmd("docker ps --format '{{.Names}},{{.ID}}'")
        return res.split("\n")

    def _is_eligible_for_doom(self, container_name):
        has_prefix = container_name.startswith(self.prefix)
        has_exeption = container_name.startswith(self.exeption_prefix)
        return container_name and has_prefix and not has_exeption and container_name != RABBITMQ_CONTAINER and container_name != EGG_OF_LIFE_CONTAINER

    def _kill(self, container_name, container_id):
        print(f"Killing {container_name}")
        run_cmd(f"docker kill {container_id}")

def main():
    if len(argv) < 3:
        print("Usage: ./hound_of_doom.py <amount_to_doom> <rest_interval_secs> [<target_prefix>] [<exeption_prefix>]")
        print("Example: ./hound_of_doom.py 2 1")
        print("Example: ./hound_of_doom.py 2 1 filter")
        print("Example: ./hound_of_doom.py 2 1 none joiner")
        return

    amount = int(argv[1])
    time_wait = int(argv[2])

    prefix = ""
    if len(argv) >= 4:
        prefix = argv[3]
        prefix = prefix if prefix != "none" else ""

    exeption_prefix = DEFAULT_EXEPTIONS
    if len(argv) >= 5:
        exeption_prefix = argv[4]

    print("Initializing Hound of Doom with config:")
    print(f"  - Amount to doom: {amount}")
    print(f"  - Time wait between dooms: {time_wait} seconds")
    print(f"  - Target prefix: '{prefix}'")
    print(f"  - Exeption prefix: '{exeption_prefix}'\n")
    hound = HoundDoom(amount, time_wait, prefix=prefix, exeption_prefix=exeption_prefix)
    hound.unleash_doom()

    print("Houndoom is finished.")

if __name__ == "__main__":
    main()

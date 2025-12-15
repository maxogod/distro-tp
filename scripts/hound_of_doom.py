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

MIN_SLEEP_INTERVAL=20
MAX_SLEEP_INTERVAL=30

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
    def __init__(self, amount_to_doom, time_wait, prefix=None, exeption_prefix=None):
        self.amount_to_doom = amount_to_doom
        self.time_wait = time_wait
        self.prefix = prefix or []
        self.exeption_prefix = exeption_prefix or []
        self.containers = []

    def unleash_doom(self):
        containers = self._get_container_names()
        self.containers = [value.strip("'").split(",") for value in containers if value]

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
        matches_target = True
        if self.prefix:
            matches_target = any(container_name.startswith(p) for p in self.prefix)

        matches_exception = any(container_name.startswith(p) for p in self.exeption_prefix)

        return (
            matches_target
            and not matches_exception
            and container_name not in (RABBITMQ_CONTAINER, EGG_OF_LIFE_CONTAINER)
        )

    def _kill(self, container_name, container_id):
        print(f"Killing {container_name}")
        run_cmd(f"docker kill {container_id}")

def main():
    if len(argv) < 3:
        print("Usage: ./hound_of_doom.py <amount_to_doom> <rest_interval_secs> [<target_prefixes>] [<exception_prefixes>]")
        print("Examples:")
        print("  ./hound_of_doom.py 2 1")
        print("  ./hound_of_doom.py 2 1 filter,joiner")
        print("  ./hound_of_doom.py 2 1 none filter,joiner")
        return

    amount = int(argv[1])
    time_wait = int(argv[2])

    prefix = []
    if len(argv) >= 4 and argv[3] != "none":
        prefix = [p for p in argv[3].split(",") if p]

    exeption_prefix = []
    if len(argv) >= 5 and argv[4] != "none":
        exeption_prefix = [p for p in argv[4].split(",") if p]

    print("Initializing Hound of Doom with config:")
    print(f"  - Amount to doom: {amount}")
    print(f"  - Time wait between dooms: {time_wait} seconds")
    print(f"  - Target prefixes: {prefix}")
    print(f"  - Exception prefixes: {exeption_prefix}\n")

    hound = HoundDoom(amount, time_wait, prefix=prefix, exeption_prefix=exeption_prefix)

    try:
        while True:
            hound.unleash_doom()
            sleep_time = random.randint(MIN_SLEEP_INTERVAL, MAX_SLEEP_INTERVAL)
            print(f"\n[Loop] Waiting {sleep_time} seconds before next doom cycle...\n")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\nHoundoom is finished.\n")

if __name__ == "__main__":
    main()

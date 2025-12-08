import subprocess
import threading
from queue import Queue
from utils.config import Config

CREATOR_LABEL = "revived_by=revival_chansey"
PROJECT = "distro"
CLOSE_SIGNAL = ""


class DockerRunner:
    def __init__(self, config: Config) -> None:
        self.config = config

        self.running = threading.Event()
        self.running.set()

        self._commands_queue: Queue[str] = Queue()

    def start(self):
        self._runner_thread = threading.Thread(target=self._run_commands, daemon=True)
        self._runner_thread.start()

    def shutdown(self, stop_containers: bool):
        if stop_containers:
            self._stop_created_containers()
        self._commands_queue.join()
        self.running.clear()
        self._commands_queue.put(CLOSE_SIGNAL)  # Unblock queue
        self._runner_thread.join()

    def restart_container(self, name: str, image: str):
        print(
            f"Launching new container {name} (image: {image}) on network {self.config.docker_network}"
        )

        if "egg_of_life" in image:
            cmd = self._get_eol_container(name)
            self._commands_queue.put(cmd)
            return

        folder_name = image.split(":")[0]

        cmd = (
            f"docker run -d --name {name} "
            f"--network {self.config.docker_network} "
            f"--label {CREATOR_LABEL} "
            f"--label com.docker.compose.project={PROJECT} "
            f"-v {self.config.host_path}/src/{folder_name}/config.yaml:/app/config.yaml "
            f"-e ID={name} -e MAX_CONTROLLER_NODES={self.config.controller_count} "
        )
        if folder_name in ["joiner", "aggregator", "controller"]:
            cmd += f"-v {self.config.host_path}/.storage/{name}:/app/storage "
        cmd += image
        print(cmd)
        self._commands_queue.put(cmd)

    def cleanup_container(self, container_name):
        self._commands_queue.put(f"docker stop {container_name} -t 1")
        self._commands_queue.put(f"docker rm -fv {container_name}")

    def _run_commands(self):
        """Run commands from the queue on another Thread"""
        while self.running.is_set():
            try:
                cmd = self._commands_queue.get()
                if cmd != CLOSE_SIGNAL:
                    self._run_cmd(cmd)
            except:
                print("Error running docker command")
            finally:
                self._commands_queue.task_done()

    def _stop_created_containers(self):
        print("Stopping created containers...")
        try:
            out = self._run_cmd(
                f"docker ps -a --filter label={CREATOR_LABEL} --format {"{{.Names}}"}"
            )
            container_names = out.splitlines()
            for name in container_names:
                self.cleanup_container(name)
        except:
            print("Error stopping created containers")

    def _run_cmd(self, cmd_str):
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

    def _get_eol_container(self, ID: str) -> str:
        """Generate docker run command for egg_of_life container"""
        cmd = (
            f"docker run -d --name {ID} "
            f"--network {self.config.docker_network} "
            f"--label {CREATOR_LABEL} "
            f"--label com.docker.compose.project={PROJECT} "
            f"-v /var/run/docker.sock:/var/run/docker.sock "
            f"-v {self.config.host_path}/src/egg_of_life/config.yaml:/app/config.yaml "
            f"-e ID={ID} "
            f"-e NETWORK={self.config.docker_network} "
            f"-e HOST_PROJECT_PATH={self.config.host_path} "
            f"-e MAX_CONTROLLER_NODES={self.config.controller_count} "
            f"-e AMOUNT_OF_NODES={self.config.amount_of_nodes} "
            f"egg_of_life:latest"
        )
        return cmd

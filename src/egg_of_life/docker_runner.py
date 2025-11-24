import subprocess
import threading
from queue import Queue

CREATOR_LABEL = "revived_by=revival_chansey"
PROJECT="distro"
CLOSE_SIGNAL=""

class DockerRunner:
    def __init__(self, network: str, host_path: str) -> None:
        self._network = network
        self._host_path = host_path

        self.running = threading.Event()
        self.running.set()

        self._commands_queue: Queue[str] = Queue()

    def start(self):
        self._runner_thread = threading.Thread(target=self._run_commands, daemon=True)
        self._runner_thread.start()

    def shutdown(self):
        self._stop_created_containers()
        self._commands_queue.join()
        self.running.clear()
        self._commands_queue.put(CLOSE_SIGNAL) # Unblock queue
        self._runner_thread.join()

    def restart_container(self, name, image):
        print(f"Launching new container {name} (image: {image}) on network {self._network}")

        folder_name = image.split(":")[0]

        cmd = (
            f"docker run -d --name {name} "
            f"--network {self._network} "
            f"--label {CREATOR_LABEL} "
            f"--label com.docker.compose.project={PROJECT} "
            f"-v {self._host_path}/src/{folder_name}/config.yaml:/app/config.yaml "
            f"{image}"
        )
        print(cmd)
        self._commands_queue.put(cmd)

    def cleanup_container(self, container_name):
        self._commands_queue.put(f"docker stop {container_name} -t 1")
        self._commands_queue.put(f"docker rm -fv {container_name}")

    def _run_commands(self):
        """ Run commands from the queue on another Thread """
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
            out = self._run_cmd(f"docker ps -a --filter label={CREATOR_LABEL} --format {"{{.Names}}"}")
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


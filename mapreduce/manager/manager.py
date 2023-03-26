"""MapReduce framework Manager node."""
import collections
import json
import logging
import os
import socket
import tempfile
import threading
import time
from typing import Any, Dict, Tuple

from mapreduce.manager.structures import (PartitionInfo, WorkerInfo,
                                          WorkerStatus)

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )

        # begin ---------------------------------------------------------
        # create an array to store all the info of workers
        self.port = port
        self.host = host
        self.finish_num = 0
        self.workers: Dict[Tuple[Any, Any], WorkerInfo] = {}
        # (host, port) -> WorkerInfo
        self.job_queue = collections.deque()
        self.task_queue = collections.deque()
        self.job_num = 0
        self.partitions = collections.deque()  # store all the Partition
        self.running = False

        tcp_thread = threading.Thread(target=self.tcp_server)
        tcp_thread.start()
        udp_thread = threading.Thread(target=self.udp_server)
        udp_thread.start()
        job_thread = threading.Thread(target=self.run_job)
        job_thread.start()

        tcp_thread.join()
        udp_thread.join()
        job_thread.join()

    def tcp_server(self):
        """Create an infinite loop to listen."""
        # Create a new TCP socket server
        with socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        ) as tcp_socket:
            tcp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            tcp_socket.bind((self.host, self.port))
            tcp_socket.listen()
            tcp_socket.settimeout(1)
            while True:
                # Accept a connection from a worker
                try:
                    conn, _ = tcp_socket.accept()
                except socket.timeout:
                    continue
                conn.settimeout(1)
                with conn:
                    message_chunks = []
                    while True:
                        try:
                            data = conn.recv(4096)
                            # print(data)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                    message_bytes = b"".join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue

                    LOGGER.debug("Received message: %s", message_dict)

                    # Add the worker to the list of registered workers
                    if message_dict["message_type"] == "register":
                        self.handle_register(message_dict)

                    # receive shutdown message,
                    # send shut down message to every worker
                    elif message_dict["message_type"] == "shutdown":
                        self.handle_shutdown()

                    # manager handle new job
                    elif message_dict["message_type"] == "new_manager_job":
                        self.handle_new_job(message_dict)

                    elif message_dict["message_type"] == "finished":
                        self.handle_finished(message_dict)
                # handle busy waiting
                time.sleep(0.1)

    def udp_server(self):
        """Create an infinite loop to listen."""
        with socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM
        ) as udp_socket:
            udp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            udp_socket.bind((self.host, self.port))
            udp_socket.settimeout(1)

            while True:
                try:
                    message_bytes = udp_socket.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                # detect whether a worker is dead
                LOGGER.debug("Received message: %s", message_dict)
                if (
                    self.workers.get(
                        (
                            message_dict["worker_host"],
                            message_dict["worker_port"],
                        )
                    )
                    is None
                ):
                    continue
                self.workers[
                    (
                        message_dict["worker_host"],
                        message_dict["worker_port"],
                    )
                ].last_heartbeat = time.time()
                for key, worker in self.workers.items():
                    if worker.status != WorkerStatus.DEAD:
                        last_time = worker.last_heartbeat
                        if time.time() - last_time > 10:
                            self.partitions.append(worker.task)
                            worker.status = WorkerStatus.DEAD
                            worker.task = None
                            worker.job_id = None
                            LOGGER.info(
                                "Worker %s:%s has died", key[0], key[1]
                            )
                time.sleep(0.1)

    def handle_register(self, message_dict):
        """Handle a register message from a worker."""
        status = WorkerStatus.READY
        # Send an acknowledgement back to the worker
        ack_msg = {
            "message_type": "register_ack",
            "worker_host": message_dict["worker_host"],
            "worker_port": message_dict["worker_port"],
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(
                    (
                        message_dict["worker_host"],
                        message_dict["worker_port"],
                    )
                )
                sock.sendall(json.dumps(ack_msg).encode("utf-8"))
            except ConnectionRefusedError:
                status = WorkerStatus.DEAD

        if (
            message_dict["worker_host"],
            message_dict["worker_port"],
        ) in self.workers:
            this_worker = self.workers[
                (message_dict["worker_host"], message_dict["worker_port"])
            ]
            # dead and re-register
            if this_worker.status == WorkerStatus.BUSY:
                # it means the worker is dead and
                # re-register (all this is before the manager detect it)
                # therefore, the manager should redistribute its task
                this_task = this_worker.task
                assert (
                    this_task is not None
                ), "BUG: this busy worker should not be None"
                self.partitions.append(this_task)
                this_worker.task = None
        LOGGER.debug(
            f"Worker {message_dict['worker_host']}:"
            + f"{message_dict['worker_port']}: {status}"
        )
        self.workers[
            (message_dict["worker_host"], message_dict["worker_port"])
        ] = WorkerInfo(
            host=message_dict["worker_host"],
            port=message_dict["worker_port"],
            status=status,
            last_heartbeat=time.time(),
            job_id=None,
            task=None,
        )

    def handle_shutdown(self):
        """Handle a shutdown message from a worker."""
        message = {"message_type": "shutdown"}
        for _, worker in self.workers.items():
            LOGGER.info(
                "Sending shutdown message to worker %s:%d",
                worker.host,
                worker.port,
            )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker.host, worker.port))
                sock.sendall(json.dumps(message).encode("utf-8"))
        LOGGER.info("Shutting down manager")
        os._exit(0)

    def handle_new_job(self, message_dict):
        """Handle a new job message from a worker."""
        LOGGER.debug("Add new job: %s", message_dict)
        job = {
            "job_id": self.job_num,
            "input_directory": message_dict["input_directory"],
            "output_directory": message_dict["output_directory"],
            "mapper_executable": message_dict["mapper_executable"],
            "reducer_executable": message_dict["reducer_executable"],
            "num_mappers": message_dict["num_mappers"],
            "num_reducers": message_dict["num_reducers"],
        }
        self.job_queue.append(job)
        self.job_num += 1

    def run_job(self):
        """Run a job in the job queue."""
        while True:
            if self.job_queue:
                job = self.job_queue.popleft()
                # runnning a job
                out_dir = job["output_directory"]
                if os.path.exists(out_dir):
                    os.rmdir(out_dir)
                os.mkdir(out_dir)
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    # Map
                    self.handle_mapping(job, tmpdir)
                    # Reduce
                    self.handle_reduce(job, tmpdir)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            time.sleep(0.1)

    def handle_mapping(self, job, tmpdir):
        """Handle mapping phase of a job."""
        # --- build partitions from input files
        files = os.listdir(job["input_directory"])
        sorted_files = sorted(files)
        file_partitions = [[] for i in range(job["num_mappers"])]
        for i, this_file in enumerate(sorted_files):
            file_partitions[i % job["num_mappers"]].append(this_file)

        for task_id, partition_files in enumerate(file_partitions):
            part = PartitionInfo(task_id=task_id, files=partition_files)
            self.partitions.append(part)
            self.finish_num += 1

        while self.finish_num:
            # loop until all tasks are finished
            if self.partitions:
                # if there are still partitions(works) to be assigned
                part = self.partitions.popleft()
                task_id = part.task_id
                this_part_files = part.files
                input_path = [
                    os.path.join(job["input_directory"], filename)
                    for filename in this_part_files
                ]
                assigned = False
                while not assigned:
                    for worker_key, worker in self.workers.items():
                        if worker.status == WorkerStatus.READY:
                            message = {
                                "message_type": "new_map_task",
                                "task_id": task_id,
                                "input_paths": input_path,
                                "executable": job["mapper_executable"],
                                "output_directory": tmpdir,
                                "num_partitions": job["num_reducers"],
                                "worker_host": worker.host,
                                "worker_port": worker.port,
                            }
                            with socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM
                            ) as sock:
                                try:
                                    sock.connect(
                                        (worker.host, worker.port)
                                    )
                                    sock.sendall(
                                        json.dumps(message).encode("utf-8")
                                    )
                                except ConnectionRefusedError:
                                    worker.status = WorkerStatus.DEAD
                                else:
                                    LOGGER.debug(
                                        "worker %s is on the work",
                                        worker_key,
                                    )
                                    worker.status = WorkerStatus.BUSY
                                    worker.task = part
                                    worker.job_id = job["job_id"]
                                    assigned = True
                                    break
                    time.sleep(0.1)
            time.sleep(0.1)

    def handle_reduce(self, job, tmpdir):
        """Handle reduce phase of a job."""
        LOGGER.info("begin Reduce Stage")
        out_dir = job["output_directory"]
        all_temp_files = os.listdir(tmpdir)
        for task_id in range(job["num_reducers"]):
            # for each reduce task, find all related files
            input_paths = []
            for filename in all_temp_files:
                if filename[-5:] == f"{task_id:05}":
                    input_paths.append(os.path.join(tmpdir, filename))
            self.partitions.append(
                PartitionInfo(task_id=task_id, files=input_paths)
            )
            self.finish_num += 1

        while self.finish_num:
            # loop until all tasks are finished
            if self.partitions:
                # if there are still partitions(works) to be assigned
                part = self.partitions.popleft()
                task_id = part.task_id
                this_part_files = part.files
                assigned = False
                while not assigned:
                    for worker_key, worker in self.workers.items():
                        if worker.status == WorkerStatus.READY:
                            # get related files from task_id
                            message = {
                                "message_type": "new_reduce_task",
                                "task_id": task_id,
                                "input_paths": this_part_files,
                                "executable": job["reducer_executable"],
                                "output_directory": out_dir,
                                "worker_host": worker.host,
                                "worker_port": worker.port,
                            }
                            LOGGER.debug(
                                "sending reduce task\n %s", message
                            )
                            with socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM
                            ) as sock:
                                try:
                                    sock.connect(
                                        (worker.host, worker.port)
                                    )
                                    sock.sendall(
                                        json.dumps(message).encode("utf-8")
                                    )
                                except ConnectionRefusedError:
                                    LOGGER.debug(
                                        "worker %s is dead", worker_key
                                    )
                                    worker.status = WorkerStatus.DEAD
                                else:
                                    LOGGER.debug(
                                        "worker %s is on the work",
                                        worker_key,
                                    )
                                    worker.status = WorkerStatus.BUSY
                                    worker.task = part
                                    worker.job_id = job["job_id"]
                                    assigned = True
                                    break
                    time.sleep(0.1)
            time.sleep(0.1)

    def handle_finished(self, message_dict):
        """Handle a finished task message."""
        self.finish_num -= 1
        for _, worker in self.workers.items():
            if worker.task.task_id == message_dict["task_id"]:
                print(111111)
                worker.status = WorkerStatus.READY
                worker.task = None
                worker.job_id = None
                break

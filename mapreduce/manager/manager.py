"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import pathlib
import mapreduce.utils
import threading
import socket
import collections
import tempfile
from typing import Any, Dict, List, Tuple
from mapreduce.manager.structures import WorkerStatus, WorkerInfo, PartitionInfo

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""
    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # begin ---------------------------------------------------------
        # create an array to store all the info of workers
        self.port = port
        self.host = host
        self.finishNum = 0
        self.workers: Dict[Tuple[Any, Any], WorkerInfo] = {}
        # (host, port) -> WorkerInfo
        self.job_queue = collections.deque()
        self.task_queue = collections.deque()
        self.job_num = 0
        self.partitions = collections.deque() # store all the Partition
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

        #   Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        # LOGGER.debug("IMPLEMENT ME!")
        # time.sleep(120)

    def tcp_server(self):
        """create an infinite loop to listen."""
        # Create a new TCP socket server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.tcp_socket:
            self.tcp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_socket.bind((self.host, self.port))
            self.tcp_socket.listen()
            self.tcp_socket.settimeout(1)
            while True:
                # Accept a connection from a worker
                try:
                    conn, addr = self.tcp_socket.accept()
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

                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue

                    LOGGER.debug("Received message: %s", message_dict)

                    # Add the worker to the list of registered workers
                    if message_dict['message_type'] == 'register':
                        self.handle_register(message_dict)

                    # receive shutdown message, send shut down message to every worker
                    elif message_dict['message_type'] == 'shutdown':
                        self.handle_shutdown()

                    # manager handle new job
                    elif message_dict['message_type'] == 'new_manager_job':
                        self.handle_new_job(message_dict)

                    elif message_dict['message_type'] == 'finished':
                        self.handle_finished(message_dict)
                # handle busy waiting
                time.sleep(0.1)

    def udp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as self.udp_socket:
            self.udp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind((self.host, self.port))
            self.udp_socket.settimeout(1)

            while True:
                try:
                    message_bytes = self.udp_socket.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                # detect whether a worker is dead
                LOGGER.debug("Received message: %s", message_dict)
                if self.workers.get((message_dict['worker_host'], 
                              message_dict['worker_port'])) is None:
                    continue
                self.workers[(message_dict['worker_host'], 
                              message_dict['worker_port'])].last_heartbeat = time.time()
                for key, worker in self.workers.items():
                    if worker.status != WorkerStatus.DEAD:
                        last_time = worker.last_heartbeat
                        if time.time() - last_time > 10:
                            self.partitions.append(worker.task)
                            worker.status = WorkerStatus.DEAD
                            worker.task = None
                            worker.job_id = None
                            LOGGER.info("Worker %s:%s has died", key[0], key[1])
                time.sleep(0.1)

    def handle_register(self, message_dict):
        # handle registration
        status = WorkerStatus.READY
        # Send an acknowledgement back to the worker
        ack_msg = {
            "message_type": "register_ack",
            "worker_host": message_dict['worker_host'],
            "worker_port": message_dict['worker_port']
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(
                    (message_dict['worker_host'], message_dict['worker_port']))
                sock.sendall(json.dumps(ack_msg).encode('utf-8'))
            except ConnectionRefusedError:
                status = WorkerStatus.DEAD
                
        if (message_dict['worker_host'], message_dict['worker_port']) in self.workers:
            this_worker = self.workers[(message_dict['worker_host'], message_dict['worker_port'])]
            # dead and re-register
            if this_worker.status == WorkerStatus.BUSY:
                # it means the worker is dead and re-register (all this is before the manager detect it)
                # therefore, the manager should redistribute its task
                this_task = this_worker.task
                assert this_task is not None, "BUG: this busy worker should not be None"
                self.partitions.append(this_task)
                this_worker.task = None
        LOGGER.debug(f"Worker {message_dict['worker_host']}:{message_dict['worker_port']}: {status}")
        self.workers[(message_dict['worker_host'], message_dict['worker_port'])] = WorkerInfo(
            host= message_dict['worker_host'],
            port= message_dict['worker_port'],
            status= status,
            last_heartbeat= time.time(),
            job_id= None,
            tasks= None,
        )

    def handle_shutdown(self):
        message = {'message_type': 'shutdown'}
        for key in self.workers:
            LOGGER.info("Sending shutdown message to worker %s:%d",
                        self.workers[key]['worker_host'], self.workers[key]['worker_port'])
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(
                    (self.workers[key]['worker_host'], self.workers[key]['worker_port']))
                sock.sendall(json.dumps(message).encode('utf-8'))
        LOGGER.info("Shutting down manager")
        os._exit(0)

    def handle_new_job(self, message_dict):
        LOGGER.debug("Add new job: %s", message_dict)
        job = {
            'job_id': self.job_num,
            'input_directory': message_dict['input_directory'],
            'output_directory': message_dict['output_directory'],
            'mapper_executable': message_dict['mapper_executable'],
            'reducer_executable': message_dict['reducer_executable'],
            'num_mappers': message_dict['num_mappers'],
            'num_reducers': message_dict['num_reducers']
        }
        self.job_queue.append(job)
        self.job_num += 1
    

    def run_job(self):
        while True:
            if self.job_queue:
                job = self.job_queue.popleft()
                # runnning a job
                out_dir = job['output_directory']
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
        #--- build partitions from input files
        files = os.listdir(job['input_directory'])
        sorted_files = sorted(files)
        file_partitions = [[] for i in range(job['num_mappers'])]
        for i, this_file in enumerate(sorted_files):
            file_partitions[i % job['num_mappers']].append(this_file)
        
        for task_id, partition_files in enumerate(file_partitions):
            part = PartitionInfo(task_id=task_id, files=partition_files)
            self.partitions.append(part)
            self.finishNum += 1

        while self.finishNum:
            # loop until all tasks are finished
            if self.partitions:
                # if there are still partitions(works) to be assigned
                part = self.partitions.popleft()
                task_id = part.task_id
                this_part_files = part.files
                input_path = [os.path.join(job['input_directory'], filename) for filename in this_part_files]
                assigned = False
                while not assigned:
                    for worker_key, worker in self.workers.items():
                        if worker.status == WorkerStatus.READY:
                            message = {
                                "message_type": "new_map_task",
                                "task_id": task_id,
                                "input_paths": input_path,
                                "executable": job['mapper_executable'],
                                "output_directory": tmpdir,
                                "num_partitions": job['num_reducers'],
                                "worker_host": self.workers[worker_key]['worker_host'],
                                "worker_port": self.workers[worker_key]['worker_port']
                            }
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                try:
                                    sock.connect(
                                        (self.workers[worker_key]['worker_host'], self.workers[worker_key]['worker_port']))
                                    sock.sendall(json.dumps(
                                        message).encode('utf-8'))
                                except ConnectionRefusedError:
                                    worker.status = WorkerStatus.DEAD
                                else:
                                    LOGGER.log("worker %s is on the work", worker_key)
                                    worker.status = WorkerStatus.BUSY
                                    worker.task = part
                                    worker.job_id = job['job_id']
                                    assigned = True
                                    break
                    time.sleep(0.1)
            time.sleep(0.1)
            assert self.finishNum == 0, "BUG: finishNum should be 0 after all tasks are finished"
            assert len(self.partitions) == 0, "BUG: partitions should be empty after all tasks are finished"
            for worker_key, worker in self.workers.items():
                # all workers should be dead, idle
                if worker.status == WorkerStatus.DEAD:
                    continue
                elif worker.status == WorkerStatus.BUSY:
                    assert False, "BUG: worker should not be busy after all mapping tasks are finished"
                elif worker.status == WorkerStatus.READY:
                    assert worker['job_id'] == None, "BUG: worker should not have job_id after all mapping tasks are finished"
                    assert worker['task'] == None, "BUG: worker should not have job_id after all mapping tasks are finished"



    def handle_reduce(self, job, tmpdir):
        LOGGER.info("begin Reduce Stage")
        out_dir = job['output_directory']
        all_temp_files = os.listdir(tmpdir)
        for task_id in range(job['num_reducers']):
            # for each reduce task, find all related files
            input_paths = []
            for filename in all_temp_files:
                if filename[-5:] == f"{task_id:05}":
                    input_paths.append(os.path.join(tmpdir, filename))
            self.partitions.append(PartitionInfo(task_id=task_id, files=input_paths))
            self.finishNum += 1

        while self.finishNum:
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
                                "executable": job['reducer_executable'],
                                "output_directory": out_dir,
                                "worker_host": worker['worker_host'],
                                "worker_port": worker['worker_port']
                            }
                            LOGGER.debug("sending reduce task\n %s", message)
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                try:
                                    sock.connect(
                                        (worker['worker_host'], worker['worker_port']))
                                    sock.sendall(json.dumps(
                                        message).encode('utf-8'))
                                except ConnectionRefusedError:
                                    LOGGER.log("worker %s is dead", worker_key)
                                    worker.status = WorkerStatus.DEAD
                                else:
                                    LOGGER.log("worker %s is on the work", worker_key)
                                    worker.status = WorkerStatus.BUSY
                                    worker.task = part
                                    worker.job_id = job['job_id']
                                    assigned = True
                                    break
                    time.sleep(0.1)
            time.sleep(0.1)
        assert self.finishNum == 0, "BUG: finishNum should be 0 after all tasks are finished"
        assert len(self.partitions) == 0, "BUG: partitions should be empty after all tasks are finished"
        for worker_key, worker in self.workers.items():
            # all workers should be dead, idle
            if worker.status == WorkerStatus.DEAD:
                continue
            elif worker.status == WorkerStatus.BUSY:
                assert False, "BUG: worker should not be busy after all mapping tasks are finished"
            elif worker.status == WorkerStatus.READY:
                assert worker['job_id'] == None, "BUG: worker should not have job_id after all mapping tasks are finished"
                assert worker['task'] == None, "BUG: worker should not have job_id after all mapping tasks are finished"
        

    def handle_finished(self, message_dict):
        self.finishNum -= 1
        for worker_key, worker in self.workers.items():
            if worker.task['task_id'] == message_dict['task_id']:
                worker.status = WorkerStatus.READY
                worker.task = None
                worker.job_id = None
                break
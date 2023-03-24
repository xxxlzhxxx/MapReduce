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
        self.shutdown = False
        self.workers = {}
        self.job_queue = collections.deque()
        self.task_queue = collections.deque()
        self.job_num = 0
        self.partitions = collections.deque()
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
            while not self.shutdown:
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

                    print(message_dict)

                    # Add the worker to the list of registered workers
                    if message_dict['message_type'] == 'register':
                        self.handle_register(message_dict)

                    # receive shutdown message, send shut down message to every worker
                    elif message_dict['message_type'] == 'shutdown':
                        self.handle_shutdown()

                    # TODO: manager handle new job
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

            while not self.shutdown:
                try:
                    message_bytes = self.udp_socket.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                # detect whether a worker is dead
                print(message_dict)
                if self.workers.get((message_dict['worker_host'], 
                              message_dict['worker_port'])) is None:
                    continue
                self.workers[(message_dict['worker_host'], 
                              message_dict['worker_port'])]['last_heartbeat'] = time.time()
                for key in self.workers:
                    if self.workers[key]['status'] != 'dead':
                        last_time = self.workers[key]['last_heartbeat']
                        if time.time() - last_time > 10:
                            self.workers[key]['status'] = 'dead'
                            self.partitions.append(self.workers[key]['tasks'])
                            self.workers[key]['tasks'] = {}
                            # print(self.partitions)
                            print(key, "has dead")

                # handle busy waiting
                time.sleep(0.1)

    def handle_register(self, message_dict):
        # handle registration
        status = 'ready'
        # Send an acknowledgement back to the worker
        # time.sleep(1)
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
                status = 'dead'
                
        if (message_dict['worker_host'], message_dict['worker_port']) in self.workers:
            if self.workers[(message_dict['worker_host'], message_dict['worker_port'])]['status'] == 'busy':
                # TODO: redistribute its task as it has dead
                self.partitions.append(self.workers[(message_dict['worker_host'], message_dict['worker_port'])]['tasks'])
                self.workers[(message_dict['worker_host'], message_dict['worker_port'])]['tasks'] = {}

        self.workers[(message_dict['worker_host'], message_dict['worker_port'])] = {
            'worker_host': message_dict['worker_host'],
            'worker_port': message_dict['worker_port'],
            'status': status,
            'tasks': {},
            'last_heartbeat': time.time(),
            'num_completed_tasks': 0
        }

    def handle_shutdown(self):
        message = {'message_type': 'shutdown'}
        for key in self.workers:
            print(key)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(
                    (self.workers[key]['worker_host'], self.workers[key]['worker_port']))
                sock.sendall(json.dumps(message).encode('utf-8'))
        self.shutdown = True
        print('shuting down manager...')

    def handle_new_job(self, message_dict):
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
        while not self.shutdown:
            finished = False
            if self.job_queue:
                job = self.job_queue.popleft()
                # runnning a job
                outdir = job['output_directory']
                if os.path.exists(outdir):
                    os.rmdir(outdir)
                os.mkdir(outdir)
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    print("create new dir in manager ", prefix)
                    while not finished and not self.shutdown:
                        # get partitions
                        files = os.listdir(job['input_directory'])
                        sorted_files = sorted(files)
                        partitions = [[] for i in range(job['num_mappers'])]
                        for i, file in enumerate(sorted_files):
                            partitions[i % job['num_mappers']].append(file)
                        for task_id, partition in enumerate(partitions):
                            part = {
                                'task_id' : task_id,
                                'partition' : partition
                            }
                            self.partitions.append(part)
                            self.finishNum += 1
                        # assign partitions to workers one by one
                        while self.finishNum:
                            if self.shutdown:
                                break
                            if self.partitions:
                                part = self.partitions.popleft()
                                task_id = part['task_id']
                                partition = part['partition']
                                input_path = [os.path.join(job['input_directory'], filename) for filename in partition]
                                print(partition)
                                assigned = False
                                while not assigned and not self.shutdown:
                                    for wroker_id in self.workers:
                                        print(wroker_id)
                                        print(self.workers[wroker_id]['status'])
                                        if self.workers[wroker_id]['status'] == 'ready':
                                            # print(key)
                                            # print(11111)
                                            message = {
                                                "message_type": "new_map_task",
                                                "task_id": task_id,
                                                "input_paths": input_path,
                                                "executable": job['mapper_executable'],
                                                "output_directory": tmpdir,
                                                "num_partitions": job['num_reducers'],
                                                "worker_host": self.workers[wroker_id]['worker_host'],
                                                "worker_port": self.workers[wroker_id]['worker_port']
                                            }
                                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                                try:
                                                    sock.connect(
                                                        (self.workers[wroker_id]['worker_host'], self.workers[wroker_id]['worker_port']))
                                                    sock.sendall(json.dumps(
                                                        message).encode('utf-8'))
                                                    self.workers[wroker_id]['status'] = 'busy'
                                                    self.workers[wroker_id]['tasks'] = part
                                                    assigned = True
                                                except ConnectionRefusedError:
                                                    self.workers[wroker_id]['status'] = 'dead'
                                            break
                                    time.sleep(0.1)
                            time.sleep(0.1)
                        # Reduce tasks
                        while job['num_reducers'] > 0 and not self.shutdown:
                            assigned = False
                            while not assigned and not self.shutdown:
                                for wroker_id in self.workers:
                                    if self.workers[wroker_id]['status'] == 'ready':
                                        # print(111111)
                                        message = {
                                            "message_type": "new_reduce_task",
                                            "task_id": job['num_reducers'] - 1,
                                            "input_directory": tmpdir,
                                            "executable": job['reducer_executable'],
                                            "output_directory": outdir,
                                            "worker_host": self.workers[wroker_id]['worker_host'],
                                            "worker_port": self.workers[wroker_id]['worker_port']
                                        }
                                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                            try:
                                                sock.connect(
                                                    (self.workers[wroker_id]['worker_host'], self.workers[wroker_id]['worker_port']))
                                                sock.sendall(json.dumps(
                                                    message).encode('utf-8'))
                                                self.workers[wroker_id]['status'] = 'busy'
                                                assigned = True
                                            except ConnectionRefusedError:
                                                self.workers[wroker_id]['status'] = 'dead'
                                        break
                                time.sleep(0.1)
                            job['num_reducers'] -= 1
                        finished = True

                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            time.sleep(0.1)

    def handle_partioning(self):
        pass

    def handle_reduce(self):
        pass

    def handle_finished(self, message_dict):
        self.finishNum -= 1
        for wroker_id in self.workers:
            # print(self.workers[wroker_id])
            if self.workers[wroker_id]['tasks']['task_id'] == message_dict['task_id']:
                self.workers[wroker_id]['status'] = 'ready'
                self.workers[wroker_id]['tasks'] = {}



@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()

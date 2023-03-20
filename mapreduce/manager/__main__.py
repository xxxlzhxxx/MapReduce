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

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # begin ---------------------------------------------------------
        # create an array to store all the info of workers
        self.port = port
        self.host = host
        self.workers = []
        self.shutdown = False
        self.workers = []
        self.job_queue = collections.deque()
        self.task_queue = collections.deque()
        self.job_num = 0
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

                    # TODO: handle input partitioning
                    elif message_dict['message_type'] == 'new_map_task':
                        self.handle_partitioning()

                    elif message_dict['message_type'] == 'finished':
                        self.handle_finished()
            
            # handle busy waiting
            time.sleep(0.1)

    def udp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as self.udp_socket:
            self.udp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            while not self.shutdown:
                self.udp_socket.bind((self.host, self.port))
                self.udp_socket.settimeout(1)
                try:
                    message_bytes = self.udp_socket.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                # detect whether a worker is dead

                # handle busy waiting
                time.sleep(0.1)

    def handle_register(self, message_dict):
        # handle registration
        status = 'active'
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
        self.workers.append({
            'worker_host': message_dict['worker_host'],
            'worker_port': message_dict['worker_port'],
            'status': status,
            'tasks': [],
            'last_heartbeat': time.time(),
            'num_completed_tasks': 0
        })

    def assigning_work(self):
        pass

    def handle_shutdown(self):
        message = {'message_type': 'shutdown'}
        for worker in self.workers:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker['worker_host'], worker['worker_port']))
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
                job = self.job_queue.pop()

                outdir = job['output_directory']
                if os.path.exists(outdir):
                    os.rmdir(outdir)
                os.mkdir(outdir)
                print("output made")
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    while not finished and not self.shutdown:
                        time.sleep(0.1)

                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            time.sleep(0.1)


    def handle_partioning(self):
        pass


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

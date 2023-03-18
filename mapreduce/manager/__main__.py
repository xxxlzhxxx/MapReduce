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
import string


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


        # begin --------------------------------------------------------------------------------
        # create an array to store all the info of workers
        self.port = port
        self.host = host
        self.workers = {}
        self.shutdown = False

        # Create a new TCP socket server
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_thread = threading.Thread(target=self.tcp_server)
        tcp_thread.start()
        tcp_thread.join()
        
        
        # Create a new UDP socket server
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_thread = threading.Thread(target=self.udp_server)
        udp_thread.start()
        udp_thread.join()


        #   Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)



    def tcp_server(self):
        """create an infinite loop to listen."""
        with self.tcp_socket:
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
                        except self.tcp_socket.timeout:
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
                        self.handle_register(message_dict, conn, addr)
                        
                    # receive shutdown message, send shut down message to every worker
                    elif message_dict['message_type'] == 'shutdown':
                        self.handle_shutdown()
                    
                    # TODO: manager handle new job
                    elif message_dict['message_type'] == 'new_manager_job':
                        self.handle_new_job()

                    # TODO: handle input partitioning
                    elif message_dict['message_type'] == 'new_map_task':
                        self.handle_partitioning()

                    elif message_dict['message_type'] == 'finished':
                        self.handle_finished()
                
            # handle busy waiting     
            time.sleep(0.1)

        

    def udp_server(self):
        with self.udp_socket:
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

                # handle busy waiting
                time.sleep(0.1)




    def handle_register(self, message_dict, conn, addr):
        # handle registration
        self.workers[addr] = {
            'worker_host': message_dict['worker_host'],
            'worker_port': message_dict['worker_port'],
            'socket': conn,
            'status': 'active',
            'tasks': [],
            'last_heartbeat': time.time(),
            'num_completed_tasks': 0
        }

        # Send an acknowledgement back to the worker
        ack_msg = {
            "message_type": "register_ack",
            "worker_host": self.workers[addr]['worker_host'],
            "worker_port": self.workers[addr]['worker_port']
        }
        conn.send(json.dumps(ack_msg).encode())


    def handle_shutdown(self):
        message = {"message_type": "shutdown"}
        for worker in self.workers:
            worker['socket'].send(json.dumps(message).encode())
        self.shutdown = True

    def handle_new_job(self):
        pass


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

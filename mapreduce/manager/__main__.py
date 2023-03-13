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

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!

        # 1.Create a new thread, which will listen for UDP heartbeat messages from the Workers.
        # Initialize the UDP server socket for receiving heartbeats from workers
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        with self.udp_socket:
            self.udp_socket.bind((host, port))

        # 2.Create a new TCP socket on the given port and call the listen() function. 
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        with self.tcp_socket:
            self.tcp_socket.bind((host, port))
            self.tcp_socket.listen()

        # Start the TCP and UDP server threads
        self.tcp_thread = threading.Thread(target=self.tcp_server)
        self.udp_thread = threading.Thread(target=self.udp_server)
        self.tcp_thread.start()
        self.udp_thread.start()


        #   Note: only one listen() thread should remain open for the whole lifetime of the Manager.

        # 4. wait for the incomming message

        # TCP thread listening


        # UDP thread listening, if one miss 5, it dies

        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)

    def tcp_server(self):
        pass

    def udp_server(self):
        pass

    def shutdown(self):
        message = {"message_type": "shutdown"}
        # send this message to all the workers


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

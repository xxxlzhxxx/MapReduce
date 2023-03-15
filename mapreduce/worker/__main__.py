"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import socket
import threading

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!

        self.port = port
        self.host = host
        self.manager_port = manager_port
        self.manager_host = manager_host

        # Create a new TCP socket server
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_thread = threading.Thread(target=self.tcp_server)
        tcp_thread.start()
        tcp_thread.join()


        # create UDP socket
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp_socket.bind((self.worker_host, self.worker_port))

        # connect to Manager
        self.tcp_socket.connect((self.manager_host, self.manager_port))
        

        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)



    def tcp_server(self):
        """create an infinite loop to listen."""
        with self.tcp_socket:
            self.tcp_socket.connect((self.manager_host, self.manager_port))


            self.tcp_socket.bind((self.host, self.port))
            self.tcp_socket.listen()

            while not self.shutdown:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection
                try:
                    conn, addr = self.tcp_socket.accept()
                except socket.timeout:
                    continue

                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                conn.settimeout(1)
                # Receive the worker's registration message

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

                    # Decode list-of-byte-strings to UTF8 and parse JSON data
                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")

                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue
                    

                
            # handle busy waiting     
            time.sleep(0.1)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()

"""MapReduce framework Worker node."""
import hashlib
import heapq
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time

LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host,
            manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.port = port
        self.host = host
        self.manager_port = manager_port
        self.manager_host = manager_host
        self.start = False

        # Create a new TCP socket server

        tcp_thread = threading.Thread(target=self.tcp_server)
        tcp_thread.start()

        udp_thread = threading.Thread(target=self.udp_send)
        udp_thread.start()

        tcp_thread.join()
        udp_thread.join()

    def udp_send(self):
        """Use UDP to send heartbeat every two second."""
        with socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM
        ) as self.udp_socket:
            # Connect to the UDP socket on server
            self.udp_socket.connect((self.manager_host, self.manager_port))
            # Send heatbeat
            message = json.dumps(
                {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            )
            while True:
                if not self.start:
                    time.sleep(0.5)
                    continue
                self.udp_socket.sendall(message.encode("utf-8"))
                time.sleep(2)

    def tcp_server(self):
        """Create an infinite loop to listen."""
        with socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        ) as self.tcp_socket:
            self.tcp_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            self.tcp_socket.bind((self.host, self.port))
            self.tcp_socket.listen()
            self.tcp_socket.settimeout(1)

            self.register()
            while True:
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

                    message_bytes = b"".join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue

                    # Add the worker to the list of registered workers
                    if message_dict["message_type"] == "register_ack":
                        self.start = True

                    elif message_dict["message_type"] == "new_map_task":
                        self.handle_mapping(message_dict)
                    elif message_dict["message_type"] == "new_reduce_task":
                        self.handle_reducing(message_dict)

                    # receive shutdown message,
                    # send shut down message to every worker
                    elif message_dict["message_type"] == "shutdown":
                        LOGGER.info("shutting down worker...")
                        os._exit(0)
                # handle busy waiting
                time.sleep(0.1)

    def register(self):
        """Send registration message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            sock.sendall(json.dumps(message).encode("utf-8"))

    def handle_mapping(self, message_dict):
        """Handle mapping task."""
        executable = message_dict["executable"]
        prefix = f"mapreduce-local-task{message_dict['task_id']:05}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            for input_path in message_dict["input_paths"]:
                LOGGER.info("Created %s", tmpdir)
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        LOGGER.info(
                            "Executed %s input=%s", executable, input_path
                        )
                        for line in map_process.stdout:
                            key, value = line.split("\t")
                            hexdigest = hashlib.md5(
                                key.encode("utf-8")
                            ).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = (
                                keyhash % message_dict["num_partitions"]
                            )
                            intermediate_file = os.path.join(
                                tmpdir,
                                f"maptask{message_dict['task_id']:05}"
                                + f"-part{partition_number:05}",
                            )
                            with open(intermediate_file, "a") as f:
                                f.write(line)
            for file in os.listdir(tmpdir):
                with open(os.path.join(tmpdir, file), "r") as f:
                    sorted_line = sorted(f.readlines())
                with open(os.path.join(tmpdir, file), "w") as f:
                    f.writelines(sorted_line)
                LOGGER.info("Sorted %s", os.path.join(tmpdir, file))
            for file in os.listdir(tmpdir):
                old_path = os.path.join(tmpdir, file)
                output_path = os.path.join(
                    message_dict["output_directory"], file
                )
                shutil.move(old_path, output_path)
                LOGGER.info("Moved %s -> %s", old_path, output_path)
        LOGGER.info("Removed %s", tmpdir)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = {
                "message_type": "finished",
                "task_id": message_dict["task_id"],
                "worker_host": self.host,
                "worker_port": self.port,
            }
            sock.sendall(json.dumps(message).encode("utf-8"))

    def handle_reducing(self, message_dict):
        """Handle reducing task."""
        LOGGER.debug(f"received\n{message_dict}")
        executable = message_dict["executable"]
        prefix = f"mapreduce-local-task{message_dict['task_id']:05}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.debug(f"Created {tmpdir}")
            prev_temp_files = []
            for f in message_dict["input_paths"]:
                prev_temp_files.append(pathlib.Path(f).resolve())
            # Merge map files
            input_streams = [open(f, "r") for f in prev_temp_files]
            merged_stream = heapq.merge(*input_streams)

            # Run reduce executable
            LOGGER.debug(f"Executed {executable}")
            output_file = os.path.join(
                tmpdir, f"part{message_dict['task_id']:05}"
            )
            with open(output_file, "w") as outfile:
                with subprocess.Popen(
                    [executable],
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                    text=True,
                ) as reduce_process:
                    for line in merged_stream:
                        reduce_process.stdin.write(line)

            # Close input_streams
            for stream in input_streams:
                stream.close()

            # Move the output file to the
            # final output directory specified by the Manager
            task_id = message_dict["task_id"]
            final_output_path = os.path.join(
                message_dict["output_directory"], f"part-{task_id:05}"
            )
            LOGGER.debug(f"Moved {output_file} -> {final_output_path}")
            shutil.move(output_file, final_output_path)

        # Remove the temporary directory
        LOGGER.debug(f"Removed {tmpdir}")

        # Send a finished message to the Manager
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = {
                "message_type": "finished",
                "task_id": message_dict["task_id"],
                "worker_host": self.host,
                "worker_port": self.port,
            }
            sock.sendall(json.dumps(message).encode("utf-8"))

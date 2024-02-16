import os
import rich
from rich.table import Table

import zmq

class LoggerNode:

    def __init__(
        self,
        node_name: str,
        logger_addr: str,
        ctx: zmq.Context = None,
    ) -> None:
        """Logger Node for logging messages from other nodes."""
        self.node_name: str = node_name
        self.logger_addr: str = logger_addr
        self.ctx: zmq.Context = ctx or zmq.Context.instance()
        self.logger_socket: zmq.Socket = self.ctx.socket(zmq.PULL)
        self.logger_socket.linger = 1
        self.logger_socket.bind(self.logger_addr)

    def __del__(self):
        self.destroy()

    def destroy(self):
        self.logger_socket.close()
        self.ctx.term()

    def run(self):
        while True:
            try:
                timestamp, level, node_name, msg = self.logger_socket.recv_multipart()
            except KeyboardInterrupt:
                break
            timestamp = timestamp.decode()
            level = level.decode()
            node_name = node_name.decode()
            msg = msg.decode()
            if level == "info":
                rich.print(f"\[{timestamp}] [blue]\[info ][/blue] \[{node_name}] {msg}")
            elif level == "warn" or level == "warning":
                rich.print(f"\[{timestamp}] [yellow]\[warn ][/yellow] \[{node_name}] {msg}")
            elif level == "error":
                rich.print(f"\[{timestamp}] [red]\[error][/red] \[{node_name}] {msg}")
            elif level == "debug":
                rich.print(f"\[{timestamp}] [green]\[debug][/green] \[{node_name}] {msg}")

import time
import json
import datetime
from binascii import hexlify
from typing import Union

import zmq

class BaseNode:

    def __init__(
        self,
        node_name: str,
        logger_addr: str,
        ctx: zmq.Context = None,
    ) -> None:
        """Base class for all nodes in the distributed system."""
        assert node_name is not None and isinstance(node_name, str)
        self.node_name: str = node_name
        self.logger_addr: str = logger_addr

        self.ctx: zmq.Context = ctx or zmq.Context.instance()
        self.logger_socket: zmq.Socket = self.ctx.socket(zmq.PUSH)
        self.logger_socket.linger = 1
        self.logger_socket.connect(self.logger_addr)
        time.sleep(0.1)  # wait for connection to establish
        self.logger(f"Connecting to logger at {self.logger_addr}")

    def __del__(self):
        self.destroy()

    def destroy(self):
        self.logger_socket.close()
        self.ctx.term()

    def fmt(self, data: Union[str, bytes, list, dict]) -> str:
        """A helper function to format the log message"""
        if isinstance(data, str):
            return data
        elif isinstance(data, bytes):
            try:
                return ("<%03d> " % len(data)) + data.decode('ascii')
            except UnicodeDecodeError:
                return ("<%03d> " % len(data)) + hexlify(data).decode('ascii')
        elif isinstance(data, (list, tuple)):
            return " ".join([self.fmt(d) for d in data])
        elif isinstance(data, dict):
            return json.dumps({
                k: self.fmt(v) for k, v in data.items()
            }, indent=4)
        else:
            return str(data)

    def logger(self, msg: Union[str, bytes, list, dict], level: str = "info") -> None:
        """Send log message to the logger socket.

        Args:
            msg (Union[str, bytes, list, dict]): The message to log.
            level (str, optional): The log level. Defaults to "info". Choices are ["debug", "info", "warning", "error"].
        Returns: None
        """
        assert level in ["debug", "info", "warning", "error"]
        timestamp: bytes = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode()
        self.logger_socket.send_multipart([
            timestamp,
            level.encode(),
            self.node_name.encode(),
            self.fmt(msg).encode(),
        ])

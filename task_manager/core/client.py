import time
from typing import Optional

import zmq

import task_manager.core.const as const
from task_manager.core.base import BaseNode

class Client(BaseNode):

    def __init__(
        self, 
        node_name: str, 
        broker_addr: str,
        logger_addr: str, 
        ctx: zmq.Context = None
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.broker_addr = broker_addr

        self.socket: zmq.Socket = None
        self.poller: zmq.Poller = zmq.Poller()

        self._reconnect_to_broker()

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
            self.socket = None
        super().destroy()
    
    def _reconnect_to_broker(self):
        if self.socket is None:
            self.logger(f"Connecting to broker at {self.broker_addr}")
        else:
            self.socket.close()
            self.poller.unregister(self.socket)
            self.socket = None
            self.logger(f"Reconnecting to broker at {self.broker_addr}")
        self.socket: zmq.Socket = self.ctx.socket(zmq.DEALER)
        self.socket.linger = 1
        self.socket.connect(self.broker_addr)
        self.poller.register(self.socket, zmq.POLLIN)
        time.sleep(0.1)

    def send(self, service_name: bytes, request: list[bytes], retry_times: int = 0, timeout: Optional[int] = None):
        reply = None
        while retry_times >= 0:
            self.socket.send_multipart([
                const.EMPTY,
                const.CALL,
                const.EMPTY,
                service_name,
                *request,
            ])
            try:
                events = dict(self.poller.poll(timeout=timeout))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                break
            if self.socket in events:
                empty, msg_type, empty, *reply = self.socket.recv_multipart()
                break
            else:
                retry_times -= 1
                if retry_times >= 0:
                    self.logger(f"retrying {retry_times} times...", level="warning")
                    self._reconnect_to_broker()
                else:
                    break
        return reply
                
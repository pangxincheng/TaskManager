import time
import threading

import zmq

import task_manager.core.const as const
from task_manager.core.base import BaseNode

class ProxyNode(BaseNode):

    def __init__(
        self, 
        node_name: str, 
        logger_addr: str, 
        node_addr: str,
        service_name: str,
        interval_addr: str = None,
        type: str = "bind",
        heartbeat_liveness: int = 3,
        heartbeat_interval: int = 2500,
        ctx: zmq.Context = None
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.node_addr: str = node_addr
        self.service_name: str = service_name
        self.interval_addr: str = interval_addr
        self.type: str = type
        self.heartbeat_liveness: int = heartbeat_liveness
        self.heartbeat_interval: int = heartbeat_interval

        self.poller: zmq.Poller = zmq.Poller()
        self.pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        if self.type == "bind": self.pair.bind(self.interval_addr)
        else: self.pair.connect(self.interval_addr)
        self.poller.register(self.pair, zmq.POLLIN)

        self.socket: zmq.Socket = None

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
            self.socket = None
        self.poller.unregister(self.pair)
        self.pair.close()
        self.pair = None
        super().destroy()

    def _test_pair(self) -> bool:
        """Test if the pair is connected."""
        while True:
            self.pair.send_multipart([const.EMPTY, const.HEARTBEAT])
            try:
                events = dict(self.poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                return False
            if self.pair in events:
                return True
        return False
    
    def _reconnect_to_socket(self):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
            self.socket = None
            self.logger(f"Reconnecting to {self.node_addr} to proxy the service {self.service_name}...", level="info")
        else:
            self.logger(f"Connecting to {self.node_addr} to proxy the service {self.service_name}...", level="info")
        self.socket = self.ctx.socket(zmq.DEALER)
        self.socket.linger = 1
        self.socket.connect(self.node_addr)
        self.poller.register(self.socket, zmq.POLLIN)
        self.socket.send_multipart([
            const.EMPTY,
            const.REGISTER,
            self.service_name.encode(),
        ])

    def run(self):
        if not self._test_pair():
            self.logger("Pair is not connected, exiting...", level="error")
            return
        print(f"ProxyNode {self.node_name} is running...")
        self._reconnect_to_socket()
        heartbeat_at = time.time() + self.heartbeat_interval / 1000
        liveness = self.heartbeat_liveness
        while True:
            t0 = time.time()
            try:
                events = dict(self.poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                return
            t1 = time.time()
            if self.socket in events:
                liveness = self.heartbeat_liveness
                empty, msg_type, *others = self.socket.recv_multipart()
                assert empty == const.EMPTY, "Empty delimiter must be const.EMPTY"
                if msg_type == const.HEARTBEAT:
                    pass
                elif msg_type == const.DISCONNECT:
                    self._reconnect_to_socket()
                    liveness = self.heartbeat_liveness
                else:
                    self.pair.send_multipart([const.EMPTY, msg_type, *others])
            if self.pair in events:
                self.socket.send_multipart(self.pair.recv_multipart())
            if t1 - t0 > self.heartbeat_interval / 1000:
                liveness -= 1
            if liveness == 0:
                self.logger(["Service name =", self.service_name, "is disconnected"])
                self._reconnect_to_socket()
                liveness = self.heartbeat_liveness
            if time.time() > heartbeat_at:
                self.socket.send_multipart([
                    const.EMPTY,
                    const.HEARTBEAT,
                ])
                heartbeat_at = time.time() + self.heartbeat_interval * 1e-3  

class RepeaterNode(BaseNode):

    def __init__(
        self, 
        node0_name: str,
        node0_addr: str,
        node1_name: str,
        node1_addr: str, 
        logger_addr: str, 
        heartbeat_liveness: int = 3,
        heartbeat_interval: int = 2500,
        ctx: zmq.Context = None
    ) -> None:
        super().__init__(node0_name + node1_name, logger_addr, ctx)
        self.node0_name: str = node0_name
        self.node1_name: str = node1_name
        self.node0_addr: str = node0_addr
        self.node1_addr: str = node1_addr
        self.heartbeat_liveness: int = heartbeat_liveness
        self.heartbeat_interval: int = heartbeat_interval
        self.interval_addr = f"inproc://{self.node_name}_interval.sock"
    
    def __del__(self):
        self.destroy()

    def destroy(self):
        super().destroy()

    def proxy_node_thread(self, node_name: str, node_addr: str, service_name: str, type: str = "bind"):
        node = ProxyNode(
            node_name=node_name,
            logger_addr=self.logger_addr,
            node_addr=node_addr,
            service_name=service_name,
            interval_addr=self.interval_addr,
            type=type,
            heartbeat_interval=self.heartbeat_interval,
            heartbeat_liveness=self.heartbeat_liveness,
            ctx=self.ctx,
        )

        node.run()

    def run(self):
        t1 = threading.Thread(
            target=self.proxy_node_thread, 
            kwargs={
                "node_name": self.node0_name + "-proxy-to-" + self.node1_name,
                "node_addr": self.node0_addr,
                "service_name": self.node1_name,
                "type": "bind",
            }
        )
        t2 = threading.Thread(
            target=self.proxy_node_thread, 
            kwargs={
                "node_name": self.node1_name + "-proxy-to-" + self.node0_name,
                "node_addr": self.node1_addr,
                "service_name": self.node0_name,
                "type": "connect",
            }
        )
        t1.start()
        t2.start()
        t1.join()
        t2.join()

import json
import time
import inspect
import threading
from typing import Union

import zmq

import task_manager.core.const as const
import task_manager.manager.utils as utils
from task_manager.core.base import BaseNode

class Worker:

    def __init__(self) -> None:
        pass

    def logger(self, msg: Union[str, bytes, list, dict], level: str = "info") -> None:
        pass

    def _run(self):
        pass

class WorkerNode(BaseNode):
    TYPE_LOGGER = b"logger"

    def __init__(
        self,
        node_name: str, 
        broker_addr: str,
        internal_addr: str,
        logger_addr: str, 
        worker_instance: Worker,
        heartbeat_liveness: int = 5,
        heartbeat_interval: int = 2500,
        ctx: zmq.Context = None
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.broker_addr: str = broker_addr
        self.internal_addr: str = internal_addr
        self.heartbeat_liveness: int = heartbeat_liveness
        self.heartbeat_interval: int = heartbeat_interval

        self.worker_instance: Worker = worker_instance

        def _predicate(fn):
            return inspect.ismethod(fn) and not fn.__name__.startswith("_") and fn.__name__ != "logger"

        self.functions = dict(
            inspect.getmembers(
                object=self.worker_instance, 
                predicate=_predicate
            )
        )

        self.dealer: zmq.Socket = None
        self.poller: zmq.Poller = zmq.Poller()

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self.dealer:
            self.poller.unregister(self.dealer)
            self.dealer.close()
            self.dealer = None
        super().destroy()

    def run(self):
        worker_thread = threading.Thread(target=self._worker_thread)
        worker_thread.start()

        # worker thread
        pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        pair.linger = 1
        pair.connect(self.internal_addr)
        self.poller.register(pair, zmq.POLLIN)
        if not self._test_pair(pair):
            self.logger("Pair is not connected, exiting...", level="error")
            return
        self._reconnect_to_broker()
        liveness = self.heartbeat_liveness
        heartbeat_at = 0.0

        while self._test_worker_alive(worker_thread):
            try:
                events = dict(self.poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                break
            if self.dealer in events:
                liveness = self.heartbeat_liveness
                empty, msg_type, *others = self.dealer.recv_multipart()
                assert empty == const.EMPTY, "Empty delimiter must be const.EMPTY"
                if msg_type == const.HEARTBEAT:
                    liveness = self.heartbeat_liveness
                elif msg_type == const.CALL:
                    pair.send_multipart([empty, msg_type, *others])
                elif msg_type == const.DISCONNECT:
                    self._reconnect_to_broker()
                else:
                    self.logger(["received unknown message type", msg_type], level="error")
            else:
                liveness -= 1
                if liveness == 0:
                    self.logger(["lost connection to broker, retrying to connect to the broker..."])
                    self._reconnect_to_broker()
                    liveness = self.heartbeat_liveness
            
            if pair in events:
                empty, msg_type, *others = pair.recv_multipart()
                if msg_type == const.HEARTBEAT:
                    pass  # ignore the heartbeat message
                elif msg_type == WorkerNode.TYPE_LOGGER:
                    level = others[0].decode()
                    others = others[1:]
                    self.logger(others, level=level)
                else:
                    self.dealer.send_multipart([empty, msg_type, *others])

            if time.time() > heartbeat_at:
                self.dealer.send_multipart([
                    const.EMPTY, 
                    const.HEARTBEAT
                ])
                heartbeat_at = time.time() + self.heartbeat_interval * 1e-3

    def _reconnect_to_broker(self):
        if self.dealer:
            self.poller.unregister(self.dealer)
            self.dealer.close()
            self.dealer = None
            self.logger(["Service name =", self.node_name, f"reconnect to {self.broker_addr}"])
        else:
            self.logger(["Service name =", self.node_name, f"connect to {self.broker_addr}"])
        self.dealer: zmq.Socket = self.ctx.socket(zmq.DEALER)
        self.dealer.linger = 1
        self.dealer.connect(self.broker_addr)
        self.poller.register(self.dealer, zmq.POLLIN)
        time.sleep(0.1)  # wait for connection to establish
        self.dealer.send_multipart([
            const.EMPTY,
            const.REGISTER,
            self.node_name.encode(),
        ])

    def _test_pair(self, pair: zmq.Socket) -> bool:
        """Test if the pair is connected."""
        while True:
            pair.send_multipart([const.EMPTY, const.HEARTBEAT])
            try:
                events = dict(self.poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                return False
            if pair in events:
                return True

    def _test_worker_alive(self, worker_thread: threading.Thread) -> bool:
        """Test if the worker thread is alive."""
        if worker_thread.is_alive():
            return True
        else:
            self.logger("Worker thread is not alive, exiting...", level="error")
            return False

    def _default_fn(self, *requests: list[bytes]) -> list[bytes]:
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": {
                    key: inspect.getdoc(value)
                    for key, value in self.functions.items()
                },
            }).encode()
        ]

    def _exception_wrapper(self, fn, *requests: list[bytes]) -> list[bytes]:
        try:
            return fn(*requests)
        except Exception as e:
            return [json.dumps({
                "status": 400,
                "msg": f"Exception when call {fn.__name__}, the excption is " + str(e)
            }).encode()]

    def _worker_thread(self):

        pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        pair.linger = 1
        pair.bind(self.internal_addr)
        time.sleep(0.1)  # wait for connection to establish

        poller: zmq.Poller = zmq.Poller()
        poller.register(pair, zmq.POLLIN)

        
        def _logger(msg: Union[str, bytes, list, dict], level: str = "info") -> None:
            try:
                pair.send_multipart([
                    const.EMPTY,
                    WorkerNode.TYPE_LOGGER,
                    level.encode(),
                    json.dumps(msg).encode(),
                ])
            except Exception as e:
                pass
        # bind the logger function to the worker_instance
        self.worker_instance._logger = _logger

        while True:
            try:
                events = dict(poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
                break

            if pair in events:
                empty, msg_type, *others = pair.recv_multipart()
                if msg_type == const.HEARTBEAT:
                    pair.send_multipart([const.EMPTY, const.HEARTBEAT])
                elif msg_type == const.CALL:
                    router_chain_len = int.from_bytes(others[0], "big") if others[0] != const.EMPTY else 0
                    router_chain = others[1:router_chain_len+1]
                    others = others[router_chain_len+1:]
                    service_name: str = others[0].decode()
                    if service_name.startswith(const.SERVICE_SPLIT):
                        service_name: str = service_name[len(const.SERVICE_SPLIT):]
                    service_name: list[str] = service_name.split(const.SERVICE_SPLIT)
                    if service_name[0] == "":
                        fn = self._default_fn
                    else:
                        fn = self.functions.get(service_name[0], self._default_fn)
                    pair.send_multipart([
                        const.EMPTY, 
                        const.REPLY,
                        router_chain_len.to_bytes(1, "big") if router_chain_len > 0 else const.EMPTY,
                        *router_chain,
                        *self._exception_wrapper(
                            fn,
                            *others[1:],
                        ),
                    ])
            self.worker_instance._run()

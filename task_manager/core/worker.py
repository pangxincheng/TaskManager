import time
import threading
from typing import Callable

import zmq

import task_manager.core.const as const
from task_manager.core.base import BaseNode

class WorkerNode(BaseNode):

    def __init__(
        self, 
        node_name: str, 
        logger_addr: str, 
        broker_addr: str = None,
        service_name: str = None,
        interval_addr: str = None,
        heartbeat_liveness: int = 3,
        heartbeat_interval: int = 2500,
        ctx: zmq.Context = None,
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.broker_addr: str = broker_addr
        self.service_name: str = service_name
        self.internal_addr: str = interval_addr if interval_addr else f"inproc:///tmp/{self.node_name}_internal.sock"
        self.heartbeat_liveness: int = heartbeat_liveness
        self.heartbeat_interval: int = heartbeat_interval

        self.dealer: zmq.Socket = None
        self.poller: zmq.Poller = zmq.Poller()

        self.hooks: list[Callable] = []

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self.dealer:
            self.poller.unregister(self.dealer)
            self.dealer.close()
        super().destroy()

    def _reconnect_to_broker(self):
        if self.dealer:
            self.poller.unregister(self.dealer)
            self.dealer.close()
            self.dealer = None
            self.logger(["Service name =", self.service_name, f"reconnect to {self.broker_addr}"])
        else:
            self.logger(["Service name =", self.service_name, f"connect to {self.broker_addr}"])
        self.dealer: zmq.Socket = self.ctx.socket(zmq.DEALER)
        self.dealer.linger = 1
        self.dealer.connect(self.broker_addr)
        self.poller.register(self.dealer, zmq.POLLIN)
        time.sleep(0.1)  # wait for connection to establish
        self.dealer.send_multipart([
            const.EMPTY,
            const.REGISTER,
            self.service_name.encode(),
        ])

    def register_hook(self, hook) -> None:
        self.hooks.append(hook)

    def callback_thread(self, callback):
        args = None
        for hook in self.hooks:
            if args is None:
                args = hook()
            else:
                args = hook(args)
        pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        pair.linger = 1
        pair.bind(self.internal_addr)

        while True:
            empty, msg_type, *others = pair.recv_multipart()
            assert msg_type == const.CALL, "Message type must be const.CALL"
            router_chain_len = int.from_bytes(others[0], "big") if others[0] != const.EMPTY else 0
            router_chain = others[1:router_chain_len+1]
            others = others[router_chain_len+1:]
            service_name: str = others[0].decode()
            if service_name.startswith(const.SERVICE_SPLIT):
                service_name: str = service_name[len(const.SERVICE_SPLIT):]
            service_name: list[str] = service_name.split(const.SERVICE_SPLIT)
            if service_name[0] != self.service_name:
                pair.send_multipart([
                    const.EMPTY,
                    const.REPLY,
                    router_chain_len.to_bytes(1, "big"),
                    *router_chain,
                    const.SERVICE_NOT_FOUND,
                ])
            else:
                msg: list[bytes] = callback(others[1:])
                pair.send_multipart([
                    const.EMPTY,
                    const.REPLY,
                    router_chain_len.to_bytes(1, "big"),
                    *router_chain,
                    *msg,
                ])

    def main_loop_thread(self):
        pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        pair.linger = 1
        pair.connect(self.internal_addr)
        self.poller.register(pair, zmq.POLLIN)
        self._reconnect_to_broker()
        self.logger(["Service name =", self.service_name, "is running..."])
        liveness = self.heartbeat_liveness
        heartbeat_at = 0.0

        while True:
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
                    self.logger(["Service name =", self.service_name, "received unknown message type", msg_type], level="error")
            else:
                liveness -= 1
                if liveness == 0:
                    self.logger(["service name =", self.service_name, "lost connection to broker"])
                    self._reconnect_to_broker()
            
            if pair in events:
                self.dealer.send_multipart(pair.recv_multipart())

            if time.time() > heartbeat_at:
                self.dealer.send_multipart([
                    const.EMPTY, 
                    const.HEARTBEAT
                ])
                heartbeat_at = time.time() + self.heartbeat_interval * 1e-3

    def run(
        self,
        callback: Callable[[list[bytes]], list[bytes]],
    ):
        self.logger(["Service name =", self.service_name, "is running..."])
        main_loop_thread = threading.Thread(target=self.main_loop_thread, daemon=True)
        main_loop_thread.start()
        self.callback_thread(callback)

    # def run(
    #     self, 
    #     callback: Callable[[list[bytes]], list[bytes]],
    # ) -> None:
    #     self._reconnect_to_broker()
    #     self.logger(["Service name =", self.service_name, "is running..."])
    #     liveness = self.heartbeat_liveness
    #     heartbeat_at = 0.0
    #     args = None
    #     for hook in self.hooks:
    #         if args is None:
    #             args = hook()
    #         else:
    #             args = hook(args)
    #     while True:
    #         try:
    #             events = dict(self.poller.poll(self.heartbeat_interval))
    #         except KeyboardInterrupt:
    #             self.logger("Interrupted caused by KeyboardInterrupt, exiting...", level="error")
    #             break

    #         if self.dealer in events:
    #             liveness = self.heartbeat_liveness
    #             empty, msg_type, *others = self.dealer.recv_multipart()
    #             assert empty == const.EMPTY, "Empty delimiter must be const.EMPTY"
    #             if msg_type == const.HEARTBEAT:
    #                 liveness = self.heartbeat_liveness
    #             elif msg_type == const.CALL:
    #                 router_chain_len = int.from_bytes(others[0], "big") if others[0] != const.EMPTY else 0
    #                 router_chain = others[1:router_chain_len+1]
    #                 others = others[router_chain_len+1:]
    #                 service_name: str = others[0].decode()
    #                 if service_name.startswith(const.SERVICE_SPLIT):
    #                     service_name: str = service_name[len(const.SERVICE_SPLIT):]
    #                 service_name: list[str] = service_name.split(const.SERVICE_SPLIT)
    #                 if service_name[0] != self.service_name:
    #                     self.dealer.send_multipart([
    #                         const.EMPTY,
    #                         const.REPLY,
    #                         router_chain_len.to_bytes(1, "big"),
    #                         *router_chain,
    #                         const.SERVICE_NOT_FOUND,
    #                     ])
    #                 else:
    #                     msg: list[bytes] = callback(others[1:])
    #                     self.dealer.send_multipart([
    #                         const.EMPTY,
    #                         const.REPLY,
    #                         router_chain_len.to_bytes(1, "big"),
    #                         *router_chain,
    #                         *msg
    #                     ])
    #             elif msg_type == const.DISCONNECT:
    #                 self._reconnect_to_broker()
    #             else:
    #                 self.logger(["Service name =", self.service_name, "received unknown message type", msg_type], level="error")
    #         else:
    #             liveness -= 1
    #             if liveness == 0:
    #                 self.logger(["service name =", self.service_name, "lost connection to broker"])
    #                 self._reconnect_to_broker()
            
    #         if time.time() > heartbeat_at:
    #             self.dealer.send_multipart([
    #                 const.EMPTY, 
    #                 const.HEARTBEAT
    #             ])
    #             heartbeat_at = time.time() + self.heartbeat_interval * 1e-3

import json
import time
from typing import Optional, Dict, List

import zmq

import task_manager.core.const as const
from task_manager.core.base import BaseNode

class NodeInfo:

    def __init__(
        self,
        identity: bytes = None,
        expiry: float = None,
        service_name: str = None,
    ) -> None:
        self.identity: bytes = identity
        self.expiry: float = expiry
        self.service_name: str = service_name

class ServiceInfo:

    def __init__(
        self,
        service_name: str = None,
        nodes_ready: List[bytes] = None,
        nodes_busy: List[bytes] = None,
        requests: List[List[bytes]] = None,
    ) -> None:
        self.service_name: str = service_name
        self.nodes_ready: List[bytes] = nodes_ready if nodes_ready is not None else []
        self.nodes_busy: List[bytes] = nodes_busy if nodes_busy is not None else []
        self.requests: List[List[bytes]] = requests if requests is not None else []

class BrokerNode(BaseNode):

    def __init__(
        self,
        node_name: str,
        logger_addr: str,
        external_addr: str,
        internal_addr: str,
        heartbeat_liveness: int = 5,
        heartbeat_interval: int = 2500,  # milliseconds
        ctx: zmq.Context = None,
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.external_addr: str = external_addr
        self.internal_addr: str = internal_addr
        self.heartbeat_liveness: int = heartbeat_liveness
        self.heartbeat_interval: int = heartbeat_interval
        self.heartbeat_at: float = time.time() + 1e-3 * self.heartbeat_interval

        self.services: Dict[str, ServiceInfo] = {}
        self.nodes: Dict[bytes, NodeInfo] = {}

        self.router: zmq.Socket = self.ctx.socket(zmq.ROUTER)
        self.router.linger = 1
        if self.external_addr is not None:
            self.router.bind(self.external_addr)
        if self.internal_addr is not None and self.internal_addr != self.external_addr:
            self.router.bind(self.internal_addr)
        time.sleep(0.1)  # wait for connection to establish
        self.logger(f"Broker bind to {self.external_addr} and {self.internal_addr}")

        self.poller: zmq.Poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)

    def __del__(self):
        self.destroy()

    def destroy(self):
        self.poller.unregister(self.router)
        self.router.close()
        return super().destroy()

    def builtin_service(self, sender_identity: bytes, router_chain: List[bytes], msg: List[bytes]) -> list[bytes]:
        """A builtin service for the broker to handle."""
        res = {}
        for service_name, service in self.services.items():
            res[service_name] = {
                "nodes_ready": len(service.nodes_ready),
                "nodes_busy": len(service.nodes_busy),
                "requests": len(service.requests),
            }
        return [json.dumps(res).encode()]

    def find_service(self, service_name: str, create_if_not_exists: bool = True) -> Optional[ServiceInfo]:
        service = self.services.get(service_name, None)
        if not service and create_if_not_exists:
            service = ServiceInfo(service_name=service_name)
            self.services[service_name] = service
        return service

    def add_node(self, node: NodeInfo) -> None:
        if node.identity not in self.nodes:
            self.nodes[node.identity] = node
        assert node.service_name in self.services, "Service must be in self.services"
        service = self.services[node.service_name]
        if node.identity in service.nodes_busy:
            service.nodes_busy.remove(node.identity)
        if node.identity not in service.nodes_ready:
            service.nodes_ready.append(node.identity)
        self.dispatch(service)

    def dispatch(self, service: ServiceInfo, msg: Optional[List[bytes]] = None) -> None:
        if msg is not None:
            service.requests.append(msg)

        self.purge_nodes(service)
        while service.nodes_ready and service.requests:
            node_identity = service.nodes_ready.pop(0)
            request = service.requests.pop(0)
            self.router.send_multipart([node_identity, const.EMPTY, *request])
            service.nodes_busy.append(node_identity)

    def purge_nodes(self, service: Optional[ServiceInfo] = None) -> None:
        if service is None:
            for service in self.services.values():
                self.purge_nodes(service)
        else:
            while service.nodes_ready:
                node_identity = service.nodes_ready[0]
                node = self.nodes.get(node_identity, None)
                assert node is not None, "Node must be in self.nodes"
                if node.expiry < time.time():
                    self.logger(["Node", node_identity, "expired"])
                    service.nodes_ready.pop(0)
                    self.nodes.pop(node_identity)
                else:
                    break
            while service.nodes_busy:
                node_identity = service.nodes_busy[0]
                node = self.nodes.get(node_identity, None)
                assert node is not None, "Node must be in self.nodes"
                if node.expiry < time.time():
                    self.logger(["Node", node_identity, "expired"])
                    service.nodes_busy.pop(0)
                    self.nodes.pop(node_identity)
                else:
                    break

    def delete_node(self, node_identity: bytes) -> None:
        self.router.send_multipart([
            node_identity,
            const.EMPTY,
            const.DISCONNECT,
        ])

    def purge_services(self) -> None:
        delete_service_name = []
        for service_name, service in self.services.items():
            if not service.nodes_ready and not service.nodes_busy and not service.requests:
                delete_service_name.append(service_name)
        if len(delete_service_name) > 0:
            for service_name in delete_service_name:
                self.services.pop(service_name)
            self.logger(["Purged services", delete_service_name])

    def send_heartbeats(self) -> None:
        if time.time() > self.heartbeat_at:
            for node in self.nodes.values():
                self.router.send_multipart([
                    node.identity,
                    const.EMPTY,
                    const.HEARTBEAT
                ])
            self.heartbeat_at = time.time() + 1e-3 * self.heartbeat_interval

    def run(self):
        self.logger("Broker start running...")
        self.heartbeat_at: float = time.time() + 1e-3 * self.heartbeat_interval
        while True:
            try:
                events = dict(self.poller.poll(self.heartbeat_interval))
            except KeyboardInterrupt:
                self.logger("Broker interrupted by KeyboardInterrupt, exiting...", level="error")
                break

            if self.router in events:
                sender_identity, empty, msg_type, *others = self.router.recv_multipart()
                assert empty == const.EMPTY, "Empty delimiter must be const.EMPTY"
                node = self.nodes.get(sender_identity, None)
                if node:  # if the sender is a node, we update its expiry
                    node.expiry = time.time() + 1e-3 * self.heartbeat_interval * self.heartbeat_liveness
                if msg_type == const.CALL:  # if the sender sends a CALL message
                    router_chain_len = int.from_bytes(others[0], "big") if others[0] != const.EMPTY else 0
                    router_chain = others[1:router_chain_len+1]
                    others = others[router_chain_len+1:]
                    service_name: str = others[0].decode()  # "/" or "/xxx" or "xxx" or ""
                    if service_name.startswith(const.SERVICE_SPLIT):
                        service_name: str = service_name[len(const.SERVICE_SPLIT):]  # "xxx" or ""
                    service_name: List[str] = service_name.split(const.SERVICE_SPLIT)
                    if service_name[0] == "":
                        msg: list[bytes] = self.builtin_service(sender_identity, router_chain, others[1:])
                        self.router.send_multipart([
                            sender_identity,
                            const.EMPTY,
                            const.REPLY,
                            router_chain_len.to_bytes(1, "big") if router_chain_len > 0 else const.EMPTY,
                            *router_chain,
                            *msg,
                        ])
                    else:
                        service = self.find_service(service_name[0], create_if_not_exists=False)
                        if not service:
                            self.router.send_multipart([
                                sender_identity,
                                const.EMPTY,
                                const.REPLY,
                                router_chain_len.to_bytes(1, "big") if router_chain_len > 0 else const.EMPTY,
                                *router_chain,
                                const.SERVICE_NOT_FOUND,
                            ])
                        else:
                            self.dispatch(
                                service=service,
                                msg=[
                                    const.CALL,
                                    (router_chain_len+1).to_bytes(1, "big"),
                                    *(router_chain + [sender_identity]),
                                    (const.SERVICE_SPLIT + const.SERVICE_SPLIT.join(service_name[1:])).encode(),
                                    *others[1:],
                                ]
                            )
                elif msg_type == const.REPLY:  # if the sender sends a REPLY message
                    if node:  # if the sender is a node
                        router_chain_len = int.from_bytes(others[0], "big") if others[0] != const.EMPTY else 0
                        router_chain = others[1:router_chain_len+1]
                        others = others[router_chain_len+1:]
                        self.router.send_multipart([
                            router_chain[-1],
                            const.EMPTY,
                            const.REPLY,
                            (router_chain_len-1).to_bytes(1, "big") if router_chain_len > 1 else const.EMPTY,
                            *router_chain[:-1],
                            *others,
                        ])
                        self.add_node(node)
                    else:  # if the sender is not a node/is a dead node
                        self.delete_node(sender_identity)
                elif msg_type == const.REGISTER:
                    if node:
                        self.logger(["Node", sender_identity, "has already registered"], level="warn")
                    else:
                        assert len(others) == 1, "The message should contain only one service name"
                        service_name: str = others[0].decode()
                        service = self.find_service(service_name)
                        node = NodeInfo(
                            identity=sender_identity,
                            expiry=time.time() + 1e-3 * self.heartbeat_interval * self.heartbeat_liveness,
                            service_name=service_name,
                        )
                        self.logger(["Node", sender_identity, "registered for service", service_name])
                        self.add_node(node)
                elif msg_type == const.DISCONNECT:
                    self.logger([sender_identity, "send a DISCONNECT message"])
                    if node: node.expiry = time.time()
                elif msg_type == const.HEARTBEAT:
                    if not node: self.delete_node(sender_identity)
                else:
                    self.logger(["Unknown message type", msg_type, "from", sender_identity], level="error")

            # purge dead nodes
            self.purge_nodes()

            # purge dead services
            self.purge_services()

            # send heartbeats
            self.send_heartbeats()

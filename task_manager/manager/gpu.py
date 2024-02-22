import math
import time
import threading

import zmq
import json
import pynvml
pynvml.nvmlInit()
import pycuda.driver as cuda_drv
cuda_drv.init()
from pycuda.tools import clear_context_caches

import task_manager.core.const as const
import task_manager.manager.utils as utils
from task_manager.core.base import BaseNode

class WatchDogManager:

    def __init__(self, chunk_size: int, unit: int) -> None:
        self.chunk_size = utils.to_bytes(chunk_size, unit)
        self._gpus = {}
        self.requests = []

    def __contains__(self, device_id: int) -> bool:
        return device_id in self._gpus
    
    def get_watchdog_info(self, device_id: int) -> dict:
        if device_id in self._gpus:
            gpu = self._gpus[device_id]
            return {
                "status": 200,
                "msg": "success",
                "chunks": [chunk[1] for chunk in gpu["chunks"]],
            }
        else:
            return {
                "status": 400,
                "msg": "not found",
            }

    def add_watchdog(self, device_id: int) -> bool:
        try:
            device = cuda_drv.Device(device_id)
            context = device.make_context()
            self._gpus[device_id] = {
                "device": device,
                "context": context,
                "nvml_handle": pynvml.nvmlDeviceGetHandleByIndex(device_id),
                "chunks": [],
            }
            return True
        except Exception as e:
            return False
    
    def remove_watchdog(self, device_id: int) -> bool:
        try:
            if device_id in self._gpus:
                gpu = self._gpus.pop(device_id)
                gpu["context"].detach()
                del gpu
                clear_context_caches()
                return True
            else:
                return False
        except Exception as e:
            return False
        
    def allocate_memory(self, device_id: int, mem_size: int):
        try:
            if device_id in self._gpus:
                gpu = self._gpus[device_id]
                total_size = sum([chunk[1] for chunk in gpu["chunks"]])
                if mem_size > total_size:
                    return False
                gpu["context"].push()
                while mem_size > 0:
                    chunk = gpu["chunks"].pop()
                    mem_size -= chunk[1]
                    chunk[0].free()
                    del chunk
                return True
            else:
                return False
        except Exception as e:
            return False

    def preempt_memory(self, device_id: int, mem_size: int):
        try:
            if device_id in self._gpus:
                gpu = self._gpus[device_id]
                nvml_handle = gpu["nvml_handle"]
                total_memory: int = pynvml.nvmlDeviceGetMemoryInfo(nvml_handle).total
                if mem_size > total_memory:
                    return 0
                else:
                    num_chunks: int = math.ceil(mem_size / self.chunk_size)
                    new_size: int = 0
                    gpu["context"].push()
                    try:
                        for _ in range(num_chunks):
                            new_chunk = cuda_drv.mem_alloc(self.chunk_size)
                            new_size += self.chunk_size
                            gpu["chunks"].append([new_chunk, self.chunk_size])
                    except Exception as e:
                        pass
                    finally:
                        cuda_drv.Context.pop()
                    return new_size
            else:
                return 0
        except Exception as e:
            pass
            return 0

    def auto_preempt_memory(self, device_id: int, mem_size: int):
        self.requests.append({
            "device_id": device_id,
            "mem_size": mem_size,
        })
        return True

    def run(self):
        for request in self.requests:
            device_id = request["device_id"]
            mem_size = request["mem_size"]
            request["mem_size"] -= self.preempt_memory(device_id, mem_size)

        for idx in range(len(self.requests) - 1, -1, -1):
            if self.requests[idx]["mem_size"] <= 0:
                del self.requests[idx]

functions = {}
def register(func):
    functions[func.__name__] = func
    return func

class GPUManager(BaseNode):
    TYPE_LOGGER = b"logger"

    def __init__(
        self,
        node_name: str,
        broker_addr: str,
        internal_addr: str,
        logger_addr: str,
        heartbeat_liveness: int = 5,
        heartbeat_interval: int = 2500,
        unit: str = "MiB",
        chunk_size: int = 512,
        ctx: zmq.Context = None,
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.service_name = node_name
        self.broker_addr = broker_addr
        self.internal_addr = internal_addr
        self.heartbeat_liveness = heartbeat_liveness
        self.heartbeat_interval = heartbeat_interval
        self.unit = unit
        self.chunk_size = chunk_size

        self.dealer: zmq.Socket = None
        self.poller: zmq.Poller = zmq.Poller()

        self.watchdogs = WatchDogManager(chunk_size, unit)

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
                elif msg_type == GPUManager.TYPE_LOGGER:
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

    def _default_fn(self, *requests: list[bytes]) -> list[bytes]:
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": sorted(list(functions.keys())),
            }).encode()
        ]

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

    def _worker_thread(self):
        pynvml.nvmlInit()

        pair: zmq.Socket = self.ctx.socket(zmq.PAIR)
        pair.linger = 1
        pair.bind(self.internal_addr)
        time.sleep(0.1)  # wait for connection to establish

        poller: zmq.Poller = zmq.Poller()
        poller.register(pair, zmq.POLLIN)

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
                    if service_name[0] != self.service_name:
                        pair.send_multipart([
                            const.EMPTY,
                            const.REPLY,
                            router_chain_len.to_bytes(1, "big"),
                            *router_chain,
                            const.SERVICE_NOT_FOUND,
                        ])
                    else:
                        if len(service_name) == 1:
                            fn = self._default_fn
                        else:
                            fn = functions.get(service_name[1], self._default_fn)
                        pair.send_multipart([
                            const.EMPTY, 
                            const.REPLY,
                            router_chain_len.to_bytes(1, "big") if router_chain_len > 0 else const.EMPTY,
                            *router_chain,
                            *self._exception_wrapper(
                                fn,
                                self,
                                *others[1:],
                            ),
                        ])

            self.watchdogs.run()

    def _exception_wrapper(self, fn, *requests: list[bytes]) -> list[bytes]:
        try:
            return fn(*requests)
        except Exception as e:
            return [json.dumps({
                "status": 400,
                "msg": f"Exception when call {fn.__name__}, the excption is " + str(e)
            }).encode()]

    @register
    def get_gpu_info(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids = requests.get("device_ids", list(range(pynvml.nvmlDeviceGetCount())))
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"

        return_msg = []
        for device_id in device_ids:
            handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
            return_msg.append({
                "device_id": device_id,
                "name": pynvml.nvmlDeviceGetName(handle),
                "total_memory": utils.fmt_size(pynvml.nvmlDeviceGetMemoryInfo(handle).total),
                "free_memory": utils.fmt_size(pynvml.nvmlDeviceGetMemoryInfo(handle).free),
                "used_memory": utils.fmt_size(pynvml.nvmlDeviceGetMemoryInfo(handle).used),
                "temperature": pynvml.nvmlDeviceGetTemperature(handle, 0),
                "power": pynvml.nvmlDeviceGetPowerUsage(handle) / 1000,
                "watchdog": self.watchdogs.get_watchdog_info(device_id),
            })
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]
    
    @register
    def add_watchdog(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        assert device_ids is not None, "device_ids should not be None"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id not in self.watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        
        return_msg = []
        for device_id in device_ids:
            if self.watchdogs.add_watchdog(device_id):
                return_msg.append({
                    "device_id": device_id,
                    "status": 200,
                    "msg": "success",
                })
            else:
                return_msg.append({
                    "device_id": device_id,
                    "status": 400,
                    "msg": "failed",
                })
        
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]
    
    @register
    def remove_watchdog(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        assert device_ids is not None, "device_ids should not be None"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id in self.watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        
        return_msg = []
        for device_id in device_ids:
            if self.watchdogs.remove_watchdog(device_id):
                return_msg.append({
                    "device_id": device_id,
                    "status": 200,
                    "msg": "success",
                })
            else:
                return_msg.append({
                    "device_id": device_id,
                    "status": 400,
                    "msg": "failed",
                })
        
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]

    @register    
    def allocate_memory(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        mem_sizes: list[int] = requests.get("mem_sizes", None)
        units: list[str] = requests.get("units", None)
        assert device_ids is not None, "device_ids should not be None"
        assert mem_sizes is not None, "mem_sizes should not be None"
        assert units is not None, "units should not be None"
        assert len(device_ids) == len(mem_sizes) and len(device_ids) == len(units), "device_ids and mem_sizes should have the same length"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id in self.watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self.watchdogs.allocate_memory(device_id, mem_size):
                return_msg.append({
                    "device_id": device_id,
                    "status": 200,
                    "msg": "success",
                })
            else:
                return_msg.append({
                    "device_id": device_id,
                    "status": 400,
                    "msg": "failed",
                })
        
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]
    
    @register
    def preempt_memory(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        mem_sizes: list[int] = requests.get("mem_sizes", None)
        units: list[str] = requests.get("units", None)
        assert device_ids is not None, "device_ids should not be None"
        assert mem_sizes is not None, "mem_sizes should not be None"
        assert units is not None, "units should not be None"
        assert len(device_ids) == len(mem_sizes) and len(device_ids) == len(units), "device_ids and mem_sizes should have the same length"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id in self.watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self.watchdogs.preempt_memory(device_id, mem_size) >= mem_size:
                return_msg.append({
                    "device_id": device_id,
                    "status": 200,
                    "msg": "success",
                })
            else:
                return_msg.append({
                    "device_id": device_id,
                    "status": 400,
                    "msg": "failed",
                })
        
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]
    
    @register
    def auto_preempt_memory(self, *requests: list[bytes]) -> list[bytes]:
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        mem_sizes: list[int] = requests.get("mem_sizes", None)
        units: list[str] = requests.get("units", None)
        assert device_ids is not None, "device_ids should not be None"
        assert mem_sizes is not None, "mem_sizes should not be None"
        assert units is not None, "units should not be None"
        assert len(device_ids) == len(mem_sizes) and len(device_ids) == len(units), "device_ids and mem_sizes should have the same length"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id in self.watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self.watchdogs.auto_preempt_memory(
                device_id=device_id, 
                mem_size=mem_size,
            ):
                return_msg.append({
                    "device_id": device_id,
                    "status": 200,
                    "msg": "success",
                })
            else:
                return_msg.append({
                    "device_id": device_id,
                    "status": 400,
                    "msg": "failed",
                })
        
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]

import math
import json

import zmq

import pynvml
pynvml.nvmlInit()
import pycuda.driver as cuda_drv
cuda_drv.init()
from pycuda.tools import clear_context_caches

from task_manager.manager import utils
from task_manager.core.worker import Worker, WorkerNode

class WatchDogManager:

    def __init__(self, chunk_size: int, unit: int) -> None:
        self._chunk_size = utils.to_bytes(chunk_size, unit)
        self._gpus = {}
        self._requests = []

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
                    num_chunks: int = math.ceil(mem_size / self._chunk_size)
                    new_size: int = 0
                    gpu["context"].push()
                    try:
                        for _ in range(num_chunks):
                            new_chunk = cuda_drv.mem_alloc(self._chunk_size)
                            new_size += self._chunk_size
                            gpu["chunks"].append([new_chunk, self._chunk_size])
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
        self._requests.append({
            "device_id": device_id,
            "mem_size": mem_size,
        })
        return True

    def run(self):
        for request in self._requests:
            device_id = request["device_id"]
            mem_size = request["mem_size"]
            request["mem_size"] -= self.preempt_memory(device_id, mem_size)

        for idx in range(len(self._requests) - 1, -1, -1):
            if self._requests[idx]["mem_size"] <= 0:
                del self._requests[idx]

class GPUWorker(Worker):

    def __init__(self, node_name: str, unit: str = "MiB", chunk_size: int = 512) -> None:
        super().__init__(node_name)
        assert unit in ["B", "MiB", "GiB", "KiB"], "Invalid unit"
        self._unit: str = unit
        self._chunk_size: int = chunk_size
        self._watchdogs = WatchDogManager(chunk_size, unit)

    def _run(self):
        self._watchdogs.run()

    def get_gpu_info(self, *requests: list[bytes]) -> list[bytes]:
        """Get GPUs information"""
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
                "watchdog": self._watchdogs.get_watchdog_info(device_id),
            })
        return [
            json.dumps({
                "status": 200,
                "msg": "success",
                "data": return_msg,
            }).encode()
        ]

    def add_watchdog(self, *requests: list[bytes]) -> list[bytes]:
        """Add watchdog to GPUs"""
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        assert device_ids is not None, "device_ids should not be None"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id not in self._watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        
        return_msg = []
        for device_id in device_ids:
            if self._watchdogs.add_watchdog(device_id):
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

    def remove_watchdog(self, *requests: list[bytes]) -> list[bytes]:
        """Remove watchdog from GPUs"""
        requests = json.loads(requests[0])
        device_ids: list[int] = requests.get("device_ids", None)
        assert device_ids is not None, "device_ids should not be None"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([isinstance(device_id, int) for device_id in device_ids]), "device_ids should be a list of integers"
        assert all([device_id >= 0 and device_id < pynvml.nvmlDeviceGetCount() for device_id in device_ids]), "device_ids should be in the valid range"
        assert all([device_id in self._watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        
        return_msg = []
        for device_id in device_ids:
            if self._watchdogs.remove_watchdog(device_id):
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

    def allocate_memory(self, *requests: list[bytes]) -> list[bytes]:
        """Allocate memory on GPUs"""
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
        assert all([device_id in self._watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self._watchdogs.allocate_memory(device_id, mem_size):
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

    def preempt_memory(self, *requests: list[bytes]) -> list[bytes]:
        """Preempt memory on GPUs"""
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
        assert all([device_id in self._watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self._watchdogs.preempt_memory(device_id, mem_size) >= mem_size:
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

    def auto_preempt_memory(self, *requests: list[bytes]) -> list[bytes]:
        """Auto preempt memory on GPUs"""
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
        assert all([device_id in self._watchdogs for device_id in device_ids]), "device_ids should not contain duplicate elements"
        assert all([isinstance(mem_size, int) for mem_size in mem_sizes]), "mem_sizes should be a list of integers"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "units should be a list of strings in ['B', 'KiB', 'MiB', 'GiB']"

        return_msg = []
        for device_id, mem_size, unit in zip(device_ids, mem_sizes, units):
            mem_size = utils.to_bytes(mem_size, unit)
            if self._watchdogs.auto_preempt_memory(
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

def create_worker(
    unit: str, 
    chunk_size: int,
    node_name: str,
    broker_addr: str,
    internal_addr: str,
    logger_addr: str,
    heartbeat_liveness: int = 5,
    heartbeat_interval: int = 2500,
    ctx: zmq.Context = None,
) -> WorkerNode:
    worker = GPUWorker(node_name, unit, chunk_size)
    worker_node = WorkerNode(
        node_name=node_name,
        broker_addr=broker_addr,
        internal_addr=internal_addr,
        logger_addr=logger_addr,
        worker_instance=worker,
        heartbeat_liveness=heartbeat_liveness,
        heartbeat_interval=heartbeat_interval,
        ctx=ctx,
    )
    return worker_node

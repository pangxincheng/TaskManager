import time
import math
import threading
import multiprocessing as mp
from typing import Dict, Any

import zmq
import pycuda.driver as pycuda_drv
from pycuda.tools import clear_context_caches

import task_manager.utils.zmq_utils as zmq_utils
import task_manager.utils.common_utils as common_utils

class GPUManager(mp.Process):

    def __init__(
        self,
        identity: str,
        device_id: int,
        gpu_manager_addr: str
    ) -> None:
        mp.Process.__init__(self, daemon=True)
        assert gpu_manager_addr.startswith("tcp://") or gpu_manager_addr.startswith("ipc://"), \
            "gpu manager address must start with tcp:// or ipc://"
        self.identity = identity
        self.device_id = device_id
        self.gpu_manager_addr = gpu_manager_addr

        self.device = None
        self.ctx = None

        self.gpu_client = None

        self.preemptive_condition = None
        self.lock_for_chunks = None
        self.zmq_context = None
        self.worker_thread = None
        self.preemptive_thread = None
        self.router_client = None
        
        self.inter_addr = f"inproc://gpu{self.device_id}_inter_server"

        self.chunks = []
        self.chunk_size = None
        self.auto_preemptive = False
        self.preemptive_interval = 5

    def __del__(self):
        if self.ctx is not None:
            try:
                self.ctx.pop()
            except Exception as e:
                pass
        clear_context_caches()

    def worker_fn(self):
        assert self.zmq_context is not None, "zmq context is not initialized"
        
        # init pycuda
        pycuda_drv.init()
        assert self.device_id < pycuda_drv.Device.count(), "Invalid device ID"
        self.device = pycuda_drv.Device(self.device_id)
        self.ctx = self.device.make_context()
        device_local_var = self.device
        ctx_local_var = self.ctx

        inter_server = zmq_utils.ZMQServer(
            addr=self.inter_addr,
            context=self.zmq_context,
        )
        while self.running:
            identity, msg = inter_server.recv_binary()
            identity = identity.decode("utf-8")
            command = common_utils.byte_msg_to_dict(msg)
            return_msg = self.exception_wrapper(
                fn=getattr(self, command["function"], self._default_fn),
                *command.get("args", {}),
                **command.get("kwargs", {})
            )
            inter_server.send_binary(
                any=common_utils.dict_to_byte_msg(return_msg),
                identity=identity,
            )

        try:
            ctx_local_var.pop()
            ctx_local_var = None
            device_local_var = None
            clear_context_caches()
        except Exception as e:
            pass

    def preemptive_fn(self):
        assert self.zmq_context is not None, "zmq context is not initialized"
        preemptive_client = zmq_utils.ZMQClient(
            addr=self.inter_addr,
            identity=f"gpu{self.device_id}_preemptive_client",
            context=self.zmq_context,
        )
        while self.running:
            with self.preemptive_condition:
                if not self.auto_preemptive:
                    self.preemptive_condition.wait()
            if self.auto_preemptive:
                preemptive_client.send_binary(
                    any=common_utils.dict_to_byte_msg({
                        "function": "get_free_memory"
                    })
                )
                msg = common_utils.byte_msg_to_dict(preemptive_client.recv_binary()[0])
                if msg["status"] == 200:
                    free_bytes = msg["result"]
                    while free_bytes > self.chunk_size:
                        chunks_len = -1
                        with self.lock_for_chunks:
                            chunks_len = len(self.chunks)
                        assert chunks_len >= 0 and self.chunk_size >= 0 and self.max_size >= 0, "Invalid chunks"
                        if (chunks_len + 1) * self.chunk_size >= self.max_size:
                            # if the total size of chunks is larger than max_size, we will not allocate more chunks
                            # and we will increase the sleep time 
                            time.sleep(self.preemptive_interval * 4)
                            if self.preemptive_auto_close:
                                self.auto_preemptive = False
                            break
                        try:
                            preemptive_client.send_binary(
                                any=common_utils.dict_to_byte_msg({
                                    "function": "mem_alloc",
                                    "kwargs": {
                                        "chunk_size": self.chunk_size,
                                        "max_size": self.chunk_size,
                                        "unit": "B"
                                    }
                                }),
                            )
                            return_msg = common_utils.byte_msg_to_dict(preemptive_client.recv_binary()[0])
                            if return_msg["status"] == 200:
                                time.sleep(self.preemptive_interval)
                            else:
                                time.sleep(self.preemptive_interval * 4)
                        except Exception as e:
                            time.sleep(self.preemptive_interval * 4)
                else:
                    time.sleep(self.preemptive_interval * 4)

    def _init_manager(self):
        self.running = True

        self.gpu_client = zmq_utils.ZMQClient(self.gpu_manager_addr, self.identity)

        self.preemptive_condition = threading.Condition()
        self.preemptive_auto_close = False
        self.lock_for_chunks = threading.Lock()
        self.zmq_context = zmq.Context()
        self.worker_thread = threading.Thread(target=self.worker_fn, daemon=True)
        self.preemptive_thread = threading.Thread(target=self.preemptive_fn, daemon=True)
        self.worker_thread.start()
        self.preemptive_thread.start()
        self.router_client = zmq_utils.ZMQClient(
            addr=self.inter_addr,
            identity=f"gpu{self.device_id}_router_client",
            context=self.zmq_context,
        )
        time.sleep(1)
        self.gpu_client.send_binary(common_utils.dict_to_byte_msg({
            "status": 200,
            "result": f"Success start a watching dog🐶 on GPU{self.device_id}"
        }))

    def run(self):
        self._init_manager()
        while self.running:
            self.router_client.send_binary(self.gpu_client.recv_binary()[0])
            self.gpu_client.send_binary(self.router_client.recv_binary()[0])
    
    def exception_wrapper(self, fn, *args, **kwargs) -> Dict[str, Any]:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return {
                "status": 400,
                "result": f"Exception when call {fn.__name__}, the excption is " + str(e)
            }

    def _default_fn(self, *args, **kwargs):
        raise NotImplementedError("This function is not implemented")

    def exit(self):
        self.running = False
        return {
            "status": 200,
            "result": "👋bye~"
        }

    def get_gpu_info(self, info_level: str="simple") -> Dict[str, Any]:
        free_bytes, total_bytes = pycuda_drv.mem_get_info()
        device_msg = {
            "device_id": self.device_id,
            "device_name": self.device.name(),
            "total_memory": common_utils.fmt_bytes(total_bytes),
            "free_memory": common_utils.fmt_bytes(free_bytes),
            "compute_capability": float("%d.%d" % self.device.compute_capability()),
            "chunk_size": "none",
            "n_chunks": -1
        }
        if self.chunk_size is not None:
            device_msg["chunk_size"] = common_utils.fmt_bytes(self.chunk_size)
            device_msg["n_chunks"] = len(self.chunks)
        if info_level != "simple":
            device_attributes_tuples = self.device.get_attributes().items()
            device_attributes = {}

            for k, v in device_attributes_tuples:
                device_attributes[str(k)] = v
            device_msg["device_attributes"] = device_attributes
        return {
            "status": 200,
            "result": device_msg
        }

    def get_free_memory(self) -> Dict[str, Any]:
        free_bytes, _ = pycuda_drv.mem_get_info()
        return {
            "status": 200,
            "result": free_bytes
        }

    def mem_alloc(self, chunk_size: int, max_size: int, unit: str="MiB") -> Dict[str, Any]:
        assert chunk_size > 0, "chunk size must be positive"
        assert max_size > 0, "max size must be positive"
        assert unit in ["B", "KiB", "MiB", "GiB"], "unit must be one of [B, KiB, MiB, GiB]"
        chunk_size = common_utils.to_bytes(chunk_size, unit)
        max_size = common_utils.to_bytes(max_size, unit)
        if self.chunk_size is not None:
            assert self.chunk_size == chunk_size, "currently the chunk size must be the same"
        free_bytes, _ = pycuda_drv.mem_get_info()
        assert chunk_size <= max_size, "chunk size must be smaller than max size"
        assert chunk_size <= free_bytes, "chunk size must be smaller than free memory"
        assert max_size <= free_bytes, "max size must be smaller than free memory"
        n_chunks = min(max_size // chunk_size, free_bytes // chunk_size)
        with self.lock_for_chunks:
            self.chunks += [pycuda_drv.mem_alloc(chunk_size) for _ in range(n_chunks)]
        self.chunk_size = chunk_size
        return {
            "status": 200,
            "result": f"Success allocate {common_utils.fmt_bytes(n_chunks * chunk_size)} memory"
        }

    def mem_release(self, mem_size: int, unit: str="MiB") -> Dict[str, Any]:
        assert mem_size > 0, "memory size must be positive"
        assert unit in ["B", "KiB", "MiB", "GiB"], "unit must be one of [B, KiB, MiB, GiB]"
        if self.chunk_size is None:
            raise Exception("mem_alloc() must be called before mem_release")
        mem_size = common_utils.to_bytes(mem_size, unit)
        if mem_size > self.chunk_size * len(self.chunks):
            raise Exception(f"memory size {common_utils.fmt_bytes(mem_size)} is too large")
        n_chunks = math.ceil(mem_size / self.chunk_size)
        with self.lock_for_chunks:
            self.chunks = self.chunks[n_chunks:]
        clear_context_caches()
        return {
            "status": 200,
            "result": f"Success release {common_utils.fmt_bytes(n_chunks * self.chunk_size)} memory"
        }

    def start_preemptive(self, chunk_size: int, max_size: int, unit: str="MiB", auto_close: bool=False):
        assert chunk_size > 0, "chunk size must be positive"
        assert max_size > 0, "max size must be positive"
        assert unit in ["B", "KiB", "MiB", "GiB"], "unit must be one of [B, KiB, MiB, GiB]"
        chunk_size = common_utils.to_bytes(chunk_size, unit)
        max_size = common_utils.to_bytes(max_size, unit)
        if self.chunk_size is not None:
            assert self.chunk_size == chunk_size, "currently the chunk size must be the same"
        self.chunk_size = chunk_size
        self.max_size = max_size
        if self.auto_preemptive:
            return {
                "status": 400,
                "result": "Already in preemptive mode, Please call stop_preemptive() first"
            }
        with self.preemptive_condition:
            self.auto_preemptive = True
            self.preemptive_auto_close = auto_close
            self.preemptive_condition.notify_all()
        return {
            "status": 200,
            "result": "Success start preemptive"
        }

    def stop_preemptive(self):
        with self.preemptive_condition:
            self.auto_preemptive = False
            self.preemptive_condition.notify_all()
        return {
            "status": 200,
            "result": "Success stop preemptive"
        }

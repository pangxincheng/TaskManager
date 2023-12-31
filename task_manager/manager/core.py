import os
import time
import multiprocessing as mp
from typing import Dict, List, Any

import pycuda.driver as pycuda_drv

import task_manager.utils.zmq_utils as zmq_utils
import task_manager.utils.common_utils as common_utils
from task_manager.manager.gpu import GPUManager
from task_manager.manager.task import TaskManager

class CoreManager(mp.Process):

    def __init__(
        self,
        core_manager_addr: str,
        gpu_manager_addr: str="ipc://gpu_manager",
        task_manager_addr: str="ipc://task_manager",
        log_dir: str="logs",
        log_level: str="INFO",
    ) -> None:
        mp.Process.__init__(self)
        assert core_manager_addr.startswith("tcp://") or core_manager_addr.startswith("ipc://"), \
            "core manager address must start with tcp:// or ipc://"
        assert gpu_manager_addr.startswith("tcp://") or gpu_manager_addr.startswith("ipc://"), \
            "gpu manager address must start with tcp:// or ipc://"
        assert task_manager_addr.startswith("tcp://") or task_manager_addr.startswith("ipc://"), \
            "task manager address must start with tcp:// or ipc://"
        self.core_manager_addr = core_manager_addr
        self.gpu_manager_addr = gpu_manager_addr
        self.task_manager_addr = task_manager_addr
        self.log_dir = log_dir
        self.log_level = log_level

    def _init_manager(self) -> None:

        self.logger = common_utils.get_logger(
            logger_name="core_manager",
            log_level=self.log_level,
            handler=os.path.join(self.log_dir, "core_manager.log")
        )

        self.logger.info(f"CoreManager is listening on {self.core_manager_addr}")
        self._core_manager = zmq_utils.ZMQServer(
            addr=self.core_manager_addr,
        )
        time.sleep(1)

        self.logger.info(f"GPUManager is listening on {self.gpu_manager_addr}")
        self._gpu_manager = zmq_utils.ZMQServer(
            addr=self.gpu_manager_addr,
        )

        self.logger.info(f"TaskManager is listening on {self.task_manager_addr}")
        self._task_manager = zmq_utils.ZMQServer(
            addr=self.task_manager_addr,
        )

        self.watched_gpus = {}
        self.watched_tasks = {}

        pycuda_drv.init()
        self.running = True

    def run(self) -> None:
        self._init_manager()
        while self.running:
            identity, msg = self._core_manager.recv_binary()
            command = common_utils.byte_msg_to_dict(msg)
            self.logger.info(f"receive command to call {command['function']}")
            return_msg = self.exception_wrapper(
                fn=getattr(self, command["function"], self._default_fn),
                *command.get("args", {}),
                **command.get("kwargs", {})
            )
            self._core_manager.send_binary(
                any=common_utils.dict_to_byte_msg(return_msg),
                identity=identity
            )

    def exception_wrapper(self, fn, *args, **kwargs) -> Dict[str, Any]:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            self.logger.error(f"Exception when call {fn.__name__}")
            self.logger.exception(e)
            return {
                "status": 400,
                "result": f"Exception when call {fn.__name__}, the excption is " + str(e)
            }
        
    def _default_fn(self, *args, **kwargs) -> None:
        raise NotImplementedError("This function is not implemented")

    def exit(self) -> Dict[str, Any]:
        self.logger.info("=> [info] exit core server...")
        self.running = False
        return_msg = {
            "watched_gpus": {},
            "watched_tasks": {}
        }
        for identity in self.watched_gpus.keys():
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "exit"
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg["watched_gpus"][identity] = msg
        for identity in self.watched_tasks.keys():
            self._task_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "exit"
                }),
                identity=identity
            )
            identity_, msg = self._task_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg["watched_tasks"][identity] = msg
        return {
            "status": 200,
            "result": {
                "msg": "👋bye~",
                "watched_gpus": return_msg
            }
        }

    def get_gpus_info_by_identities(self, identities: List[str], info_level: str="simple") -> Dict[str, Any]:
        if len(identities) == 0:
            identities = list(self.watched_gpus.keys())
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"

        return_msg = {}
        for identity in identities:
            if identity not in self.watched_gpus.keys():
                return_msg[identity] = {
                    "status": 400,
                    "result": f"Could not find a watch dog with identity {identity}"
                }
            else:
                self._gpu_manager.send_binary(
                    any=common_utils.dict_to_byte_msg({
                        "function": "get_gpu_info",
                        "kwargs": {
                            "info_level": info_level
                        }
                    }),
                    identity=identity
                )
                identity_, msg = self._gpu_manager.recv_binary()
                identity_ = identity_.decode("utf-8")
                msg = common_utils.byte_msg_to_dict(msg)
                assert identity == identity_, "identity mismatch"
                return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
    
    def get_gpus_info_by_device_ids(self, device_ids: List[int], info_level: str="simple") -> Dict[str, Any]:
        if len(device_ids) == 0:
            device_ids = list(range(pycuda_drv.Device.count()))
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([device_id >= 0 and device_id < pycuda_drv.Device.count() for device_id in device_ids]), \
            "The device_id should be in the valid range"
        watched_gpu_device_ids = {
            self.watched_gpus[identity]["device_id"]: identity
            for identity in self.watched_gpus.keys()
            if self.watched_gpus[identity]["device_id"] in device_ids
        }
        unwatched_gpus = sorted(list(set(device_ids) - watched_gpu_device_ids.keys()))

        return_msg = {}
        for device_id in watched_gpu_device_ids.keys():
            identity = watched_gpu_device_ids[device_id]
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "get_gpu_info",
                    "kwargs": {
                        "info_level": info_level
                    }
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
        
        return_msg["unwatched"] = []
        for device_id in unwatched_gpus:
            gpu_device = pycuda_drv.Device(device_id)
            device_msg = {
                "device_id": device_id,
                "device_name": gpu_device.name(),
                "total_memory": common_utils.fmt_bytes(gpu_device.total_memory()),
                "compute_capability": float("%d.%d" % gpu_device.compute_capability()),
            }
            if info_level != "simple":
                device_attributes_tuples = gpu_device.get_attributes().items()
                device_attributes = {}

                for k, v in device_attributes_tuples:
                    device_attributes[str(k)] = v
                device_msg["device_attributes"] = device_attributes
            return_msg["unwatched"].append(device_msg)
        
        return {
            "status": 200,
            "result": return_msg
        }
    
    def start_watch_dog_by_device_ids(self, device_ids: List[int]) -> Dict[str, Any]:
        assert len(device_ids) > 0, "device_ids should not be empty"
        assert len(device_ids) == len(set(device_ids)), "device_ids should not contain duplicate elements"
        assert all([device_id >= 0 and device_id < pycuda_drv.Device.count() for device_id in device_ids]), \
            "The device_id should be in the valid range"
        watched_gpu_device_ids = {
            self.watched_gpus[identity]["device_id"]: identity
            for identity in self.watched_gpus.keys()
            if self.watched_gpus[identity]["device_id"] in device_ids
        }
        return_msg = {}
        for device_id in device_ids:
            if device_id in watched_gpu_device_ids.keys():
                return_msg[watched_gpu_device_ids[device_id]] = {
                    "status": 400,
                    "result": f"GPU{device_id} is already being watched by {watched_gpu_device_ids[device_id]}"
                }
            else:
                timestamp = str(time.time())
                identity = common_utils.md5(f"watch_dog_{device_id}_{timestamp}")
                watchdog = GPUManager(
                    identity=identity,
                    device_id=device_id,
                    gpu_manager_addr=self.gpu_manager_addr,
                )
                watchdog.start()
                identity_, msg = self._gpu_manager.recv_binary()
                identity_ = identity_.decode("utf-8")
                msg = common_utils.byte_msg_to_dict(msg)
                assert identity == identity_, "identity mismatch"
                self.watched_gpus[identity] = {
                    "device_id": device_id,
                    "watchdog": watchdog,
                    "timestamp": timestamp
                }
                return_msg[identity] = msg
            
        return {
            "status": 200,
            "result": return_msg
        }

    def stop_watch_dog_by_identities(self, identities: List[str]) -> Dict[str, Any]:
        if len(identities) == 0:
            identities = list(self.watched_gpus.keys())
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        
        return_msg = {}
        for identity in identities:
            if identity not in self.watched_gpus.keys():
                return_msg[identity] = {
                    "status": 400,
                    "result": f"Could not find a watch dog with identity {identity}"
                }
            else:
                self._gpu_manager.send_binary(
                    any=common_utils.dict_to_byte_msg({
                        "function": "exit",
                    }),
                    identity=identity
                )
                identity_, msg = self._gpu_manager.recv_binary()
                identity_ = identity_.decode("utf-8")
                msg = common_utils.byte_msg_to_dict(msg)
                assert identity == identity_, "identity mismatch"
                self.watched_gpus[identity]["watchdog"].join()
                del self.watched_gpus[identity]
                return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
    
    def mem_alloc_by_identities(self, identities: List[str], chunk_sizes: List[int], max_sizes: List[int], units: List[str]) -> Dict[str, Any]:
        assert len(identities) == len(chunk_sizes) == len(max_sizes) == len(units), "The lengths of identities, chunk_sizes, max_sizes and units should be equal"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "The unit should be one of B, KiB, MiB and GiB"
        assert all([chunk_size > 0 for chunk_size in chunk_sizes]), "The chunk_size should be positive"
        assert all([max_size > 0 for max_size in max_sizes]), "The max_size should be positive"
        assert all([chunk_size <= max_size for chunk_size, max_size in zip(chunk_sizes, max_sizes)]), "The chunk_size should be less than or equal to max_size"
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert all([identity in self.watched_gpus.keys() for identity in identities]), "The identity should be in the valid range"
        return_msg = {}
        for identity, chunk_size, max_size, unit in zip(identities, chunk_sizes, max_sizes, units):
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "mem_alloc",
                    "kwargs": {
                        "chunk_size": chunk_size,
                        "max_size": max_size,
                        "unit": unit,
                    }
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
    
    def mem_release_by_identities(self, identities: List[str], mem_sizes: List[int], units: List[str]) -> Dict[str, Any]:
        if len(identities) == 0:
            identities = list(self.watched_gpus.keys())
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert len(identities) == len(mem_sizes) == len(units), "The lengths of identities, mem_sizes and units should be equal"
        assert all([identity in self.watched_gpus.keys() for identity in identities]), "The identity should be in the valid range"
        assert all([mem_size > 0 for mem_size in mem_sizes]), "The mem_size should be positive"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "The unit should be one of B, KiB, MiB and GiB"
        return_msg = {}
        for identity, mem_size, unit in zip(identities, mem_sizes, units):
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "mem_release",
                    "kwargs": {
                        "mem_size": mem_size,
                        "unit": unit,
                    }
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
    
    def start_preemptive_by_identities(self, identities, chunk_sizes: List[int], max_sizes: List[int], units: List[str], auto_close: bool=False) -> Dict[str, Any]:
        assert len(identities) == len(chunk_sizes) == len(max_sizes) == len(units), "The lengths of identities, chunk_sizes, max_sizes and units should be equal"
        assert all([unit in ["B", "KiB", "MiB", "GiB"] for unit in units]), "The unit should be one of B, KiB, MiB and GiB"
        assert all([chunk_size > 0 for chunk_size in chunk_sizes]), "The chunk_size should be positive"
        assert all([max_size > 0 for max_size in max_sizes]), "The max_size should be positive"
        assert all([chunk_size <= max_size for chunk_size, max_size in zip(chunk_sizes, max_sizes)]), "The chunk_size should be less than or equal to max_size"
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert all([identity in self.watched_gpus.keys() for identity in identities]), "The identity should be in the valid range"
        return_msg = {}
        for identity, chunk_size, max_size, unit in zip(identities, chunk_sizes, max_sizes, units):
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "start_preemptive",
                    "kwargs": {
                        "chunk_size": chunk_size,
                        "max_size": max_size,
                        "unit": unit,
                        "auto_close": auto_close,
                    }
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
    
    def stop_preemptive_by_identities(self, identities: List[str]) -> Dict[str, Any]:
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert all([identity in self.watched_gpus.keys() for identity in identities]), "The identity should be in the valid range"
        return_msg = {}
        for identity in identities:
            self._gpu_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "stop_preemptive",
                }),
                identity=identity
            )
            identity_, msg = self._gpu_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }

    def add_task(self, user_args: List[str], stdout_file: str, stderr_file: str) -> Dict[str, Any]:
        assert len(user_args) > 0, "user_args should not be empty"
        timestamp = str(time.time())
        identity = common_utils.md5("_".join(user_args) + timestamp)
        watchdog = TaskManager(
            identity=identity,
            core_manager_addr=self.core_manager_addr,
            task_manager_addr=self.task_manager_addr,
            user_args=user_args,
            stdout_file=stdout_file,
            stderr_file=stderr_file
        )
        watchdog.start()
        identity_, msg = self._task_manager.recv_binary()
        identity_ = identity_.decode("utf-8")
        msg = common_utils.byte_msg_to_dict(msg)
        assert identity == identity_, "identity mismatch"
        self.watched_tasks[identity] = {
            "watchdog": watchdog,
            "user_args": user_args,
            "timestamp": timestamp,
        }
        return {
            "status": 200,
            "result": {
                identity: msg
            }
        }

    def remove_task_by_task_daemon(self, identity: str, msg: str, return_code: int) -> Dict[str, Any]:
        assert identity in self.watched_tasks.keys(), "The identity should be in the valid range"
        watchdog = self.watched_tasks[identity]["watchdog"]
        watchdog.join()
        del self.watched_tasks[identity]
        return {
            "status": 200,
            "result": {
                "identity": identity,
                "msg": msg,
                "return_code": return_code,
            }
        }

    def remove_tasks(self, identities: List[str]) -> Dict[str, Any]:
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert all([identity in self.watched_tasks.keys() for identity in identities]), "The identity should be in the valid range"
        return_msg = {}
        for identity in identities:
            self._task_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "exit",
                }),
                identity=identity
            )
            identity_, msg = self._task_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            return_msg[identity] = msg
            self.watched_tasks[identity]["watchdog"].join()
            del self.watched_tasks[identity]
        return {
            "status": 200,
            "result": return_msg
        }

    def get_task_info_by_identities(self, identities: List[str]) -> Dict[str, Any]:
        if len(identities) == 0:
            identities = list(self.watched_tasks.keys())
        assert len(identities) == len(set(identities)), "identities should not contain duplicate elements"
        assert all([identity in self.watched_tasks.keys() for identity in identities]), "The identity should be in the valid range"
        return_msg = {}
        for identity in identities:
            self._task_manager.send_binary(
                any=common_utils.dict_to_byte_msg({
                    "function": "get_status",
                }),
                identity=identity
            )
            identity_, msg = self._task_manager.recv_binary()
            identity_ = identity_.decode("utf-8")
            msg = common_utils.byte_msg_to_dict(msg)
            assert identity == identity_, "identity mismatch"
            msg["result"]["user_args"] = self.watched_tasks[identity]["user_args"]
            msg["result"]["timestamp"] = self.watched_tasks[identity]["timestamp"]
            return_msg[identity] = msg
        return {
            "status": 200,
            "result": return_msg
        }
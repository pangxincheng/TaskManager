import os
import time
import math
import signal
import threading
import subprocess
import multiprocessing as mp
from typing import Dict, Any

import task_manager.utils.zmq_utils as zmq_utils
import task_manager.utils.common_utils as common_utils

class TaskManager(mp.Process):

    def __init__(
        self, 
        identity: str, 
        core_manager_addr: str,
        task_manager_addr: str,
        user_args: str,
        stdout_file: str,
        stderr_file: str,
    ) -> None:
        mp.Process.__init__(self, daemon=True)
        assert core_manager_addr.startswith("tcp://") or core_manager_addr.startswith("ipc://"), \
            "core_manager_addr must start with tcp:// or ipc://"
        assert task_manager_addr.startswith("tcp://") or task_manager_addr.startswith("ipc://"), \
            "task_manager_addr must start with tcp:// or ipc://"
        self.identity = identity
        self.core_manager_addr = core_manager_addr
        self.task_manager_addr = task_manager_addr
        self.user_args = user_args
        self.process_lock = None
        self.process = None
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file

    def daemon_fn(self):
        daemon_client = zmq_utils.ZMQClient(
            addr=self.core_manager_addr,
            identity=self.identity,
        )
        time.sleep(1)
        while self.running:
            poll_status = None
            with self.process_lock:
                if self.process is not None:
                    poll_status = self.process.poll()
            if poll_status is not None:
                daemon_client.send_binary(
                    any=common_utils.dict_to_byte_msg({
                        "function": "remove_task_by_task_daemon",
                        "kwargs": {
                            "identity": self.identity,
                            "msg": "Task finished",
                            "return_code": poll_status,
                        }
                    })
                )
                self.running = False
            else:
                time.sleep(5)

    def signal_handler(self, signum, frame):
        exit(0)

    def _init_manager(self) -> None:
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.running = True
        self.task_client = zmq_utils.ZMQClient(
            addr=self.task_manager_addr,
            identity=self.identity,
        )
        time.sleep(1)
        self.stdout = open(self.stdout_file, "wb")
        if self.stdout_file == self.stderr_file:
            self.stderr = self.stdout
        else:
            self.stderr = open(self.stderr_file, "wb")
        self.process_lock = threading.Lock()
        self.process = subprocess.Popen(
            self.user_args,
            stdout=self.stdout,
            stderr=self.stderr,
            start_new_session=True,
        )
        self.task_client.send_binary(common_utils.dict_to_byte_msg({
            "status": 200,
            "result": f"Success start a watching dog🐶 to run {' '.join(self.user_args)}"
        }))
        time.sleep(1)
        self.daemon_thread = threading.Thread(target=self.daemon_fn, daemon=True)
        self.daemon_thread.start()

    def run(self):
        self._init_manager()
        while self.running:
            msg = self.task_client.recv_binary()[0]
            command = common_utils.byte_msg_to_dict(msg)
            return_msg = self.exception_wrapper(
                fn=getattr(self, command["function"], self._default_fn),
                *command.get("args", {}),
                **command.get("kwargs", {})
            )
            self.task_client.send_binary(
                any=common_utils.dict_to_byte_msg(return_msg),
            )

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
        with self.process_lock:
            os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            return_code = self.process.wait()
        self.stdout.close()
        if self.stdout_file != self.stderr_file:
            self.stderr.close()
        return {
            "status": 200,
            "result": {
                "msg": "👋bye~",
                "return_code": return_code,
            }
        }

    def get_status(self):
        process_status = None
        with self.process_lock:
            if self.process is not None:
                process_status = self.process.poll()
        if process_status is None:
            return {
                "status": 200,
                "result": {
                    "status": "running"
                }
            }
        else:
            return {
                "status": 200,
                "result": {
                    "status": "finished"
                }
            }

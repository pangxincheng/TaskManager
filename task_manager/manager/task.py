import json
import time
import psutil
import subprocess
import threading

import zmq

import task_manager.core.const as const
import task_manager.manager.utils as utils
from task_manager.core.base import BaseNode

functions = {}
def register(func):
    functions[func.__name__] = func
    return func

# Adapted from https://psutil.readthedocs.io/en/latest/#kill-process-tree
def terminate_process_tree(pid):
    process = psutil.Process(pid)
    children = process.children(recursive=True)
    children.append(process)
    for child in children:
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children, timeout=30)
    for p in alive:
        p.kill()

class WatchdogManager:

    def __init__(self) -> None:
        self._tasks = {}

    def __del__(self):
        for task_id in self._tasks:
            if self._tasks[task_id]["status"] == "running":
                self.kill_task(task_id)
    
    def create_task(self, task_id: str, args: list[str], stdout=None, stderr=None):
        self._tasks[task_id] = {
            "args": args,
            "status": "running",
            "start_time": time.time(),
            "end_time": None,
            "stdout": open(stdout, "w") if stdout else subprocess.PIPE,
            "stderr": open(stderr, "w") if stderr else subprocess.PIPE,
        }
        task_handler = subprocess.Popen(
            args=args, 
            stdout=self._tasks[task_id]["stdout"], 
            stderr=self._tasks[task_id]["stderr"], 
        )
        self._tasks[task_id]["handler"] = task_handler
        self._tasks[task_id]["pid"] = task_handler.pid

    def kill_task(self, task_id: str) -> str:
        if task_id not in self._tasks:
            return "not found"
        terminate_process_tree(self._tasks[task_id]["handler"].pid)
        if self._tasks[task_id]["stdout"] != subprocess.PIPE:
            self._tasks[task_id]["stdout"].close()
        if self._tasks[task_id]["stderr"] != subprocess.PIPE:
            self._tasks[task_id]["stderr"].close()
        self._tasks[task_id]["status"] = "killed"
        self._tasks[task_id]["end_time"] = time.time()
        return "killed"
    
    def get_task_info(self, task_id: str) -> str:
        if task_id not in self._tasks:
            return "not found"
        return {
            "args": self._tasks[task_id]["args"],
            "status": self._tasks[task_id]["status"],
            "start_time": self._tasks[task_id]["start_time"],
            "end_time": self._tasks[task_id]["end_time"],
            "pid": self._tasks[task_id]["pid"],
        }
    
    def clear_tasks(self):
        for task_id in list(self._tasks.keys()):
            if self._tasks[task_id]["end_time"]:
                self._tasks.pop(task_id)
    
    def run(self):
        for task_id in self._tasks:
            if self._tasks[task_id]["status"] == "running":
                if self._tasks[task_id]["handler"].poll() is not None:
                    self._tasks[task_id]["status"] = "finished"
                    if self._tasks[task_id]["stdout"] != subprocess.PIPE:
                        self._tasks[task_id]["stdout"].close()
                    if self._tasks[task_id]["stderr"] != subprocess.PIPE:
                        self._tasks[task_id]["stderr"].close()
                    self._tasks[task_id]["end_time"] = time.time()

        for task_id in list(self._tasks.keys()):
            if self._tasks[task_id]["end_time"] and time.time() - self._tasks[task_id]["end_time"] > 3600:
                self._tasks.pop(task_id)

class TaskManager(BaseNode):
    TYPE_LOGGER = b"logger"

    def __init__(
        self, 
        node_name: str, 
        broker_addr: str,
        internal_addr: str,
        logger_addr: str, 
        heartbeat_liveness: int = 5,
        heartbeat_interval: int = 2500,
        ctx: zmq.Context = None
    ) -> None:
        super().__init__(node_name, logger_addr, ctx)
        self.service_name = node_name
        self.broker_addr = broker_addr
        self.internal_addr = internal_addr
        self.heartbeat_liveness = heartbeat_liveness
        self.heartbeat_interval = heartbeat_interval

        self.dealer: zmq.Socket = None
        self.poller: zmq.Poller = zmq.Poller()

        self.watchdogs = WatchdogManager()

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
                elif msg_type == TaskManager.TYPE_LOGGER:
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
    def create_task(self, *requests: list[bytes]) -> list[bytes]:
        """
        Create a task.
        """
        requests = json.loads(requests[0])
        assert "args" in requests, "args must be in the requests"
        task_id = utils.get_uuid()
        args = requests["args"]
        stdout = requests.get("stdout", None)
        stderr = requests.get("stderr", None)
        self.watchdogs.create_task(task_id, args, stdout=stdout, stderr=stderr)
        return [json.dumps({
            "status": 200,
            "msg": "success",
            "data": task_id,
        }).encode()]
    
    @register
    def kill_task(self, *requests: list[bytes]) -> list[bytes]:
        """
        Kill a task.
        """
        requests = json.loads(requests[0])
        task_ids = requests.get("task_ids", [])
        return_msg = []
        for task_id in task_ids:
            task_status = self.watchdogs.kill_task(task_id)
            return_msg.append({
                "task_id": task_id,
                "status": task_status,
            })
        return [json.dumps({
            "status": 200,
            "msg": "success",
            "data": return_msg,
        }).encode()]
    
    @register
    def get_task_info(self, *requests: list[bytes]) -> list[bytes]:
        """
        Get task status.
        """
        requests = json.loads(requests[0])
        task_ids = requests.get("task_ids", [])
        return_msg = []
        for task_id in task_ids:
            task_status = self.watchdogs.get_task_info(task_id)
            return_msg.append({
                "task_id": task_id,
                "data": task_status,
            })

        return [json.dumps({
            "status": 200,
            "msg": "success",
            "data": return_msg,
        }).encode()]

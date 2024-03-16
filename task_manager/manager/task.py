import json
import time
import enum
import psutil
import datetime
import subprocess
from collections import OrderedDict

import zmq

import pymysql
import pymysql.cursors
from task_manager.manager import utils
from task_manager.core.worker import Worker, WorkerNode

# Adapted from https://psutil.readthedocs.io/en/latest/#kill-process-tree
def terminate_process_tree(pid):
    try:
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
    except psutil.NoSuchProcess:
        pass

class TaskWorker(Worker):

    class TASK_STATUS(enum.Enum):
        waiting = 0
        running = 1
        killing = 3
        finished = 4

    def __init__(
        self,
        node_name,
        max_task_num: int,
        mysql_host: str,
        mysql_user: str,
        mysql_password: str,
        mysql_database: str,
        mysql_charset: str = "utf8"
    ) -> None:
        super().__init__(node_name)
        self.max_task_num = max_task_num
        self.sql_conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset=mysql_charset,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.sql_conn.cursor()
        self._tasks = OrderedDict()

    def __del__(self):
        try:
            self.sql_conn.close()
        except Exception as e:
            pass

    def _run(self):
        # try to kill the finished or killing tasks
        for task_id, task in self._tasks.items():
            if task["status"] == TaskWorker.TASK_STATUS.running:
                handler = task["handler"]
                if handler.poll() is not None:
                    task["status"] = TaskWorker.TASK_STATUS.finished
                    task["end_time"] = time.time()
                    task["pid"] = handler.pid
                    task["return_code"] = handler.returncode
                    if task["stdout"] != subprocess.PIPE: task["stdout"].close()
                    if task["stderr"] != subprocess.PIPE: task["stderr"].close()
            elif task["status"] == TaskWorker.TASK_STATUS.killing:
                handler = task["handler"]
                if handler.poll() is None:
                    terminate_process_tree(handler.pid)
                else:
                    task["status"] = TaskWorker.TASK_STATUS.finished
                    task["end_time"] = time.time()
                    task["pid"] = handler.pid
                    task["return_code"] = handler.returncode
                    if task["stdout"] != subprocess.PIPE: task["stdout"].close()
                    if task["stderr"] != subprocess.PIPE: task["stderr"].close()

        # try to start a new task
        has_running = False
        for task_id, task in self._tasks.items():
            if task["status"] == TaskWorker.TASK_STATUS.running:
                has_running = True
                break
        if not has_running:
            # try to start a new task
            for task_id, task in self._tasks.items():
                if task["status"] == TaskWorker.TASK_STATUS.waiting:
                    task["status"] = TaskWorker.TASK_STATUS.running
                    task["handler"] = subprocess.Popen(task["args"], stdout=task["stdout"], stderr=task["stderr"])
                    task["pid"] = task["handler"].pid
                    break

        # save the finished tasks to sql
        for task_id  in list(self._tasks.keys()):
            task = self._tasks[task_id]
            if task["status"] == TaskWorker.TASK_STATUS.finished:
                self._write_to_sql(task_id, task)
                del self._tasks[task_id]

    def _write_to_sql(self, task_id: str, task: dict) -> None:
        sql = """
        insert into task(
            uuid, task_name, args, uid, start_time, end_time, node_name, return_code, pid
        )
        values(
            %(uuid)s, %(task_name)s, %(args)s, %(uid)s, %(start_time)s, %(end_time)s, %(node_name)s, %(return_code)s, %(pid)s
        )
        """
        try:
            self.cursor.execute(sql, {
                "uuid": task_id,
                "task_name": task["task_name"],
                "args": json.dumps(task["args"]),
                "uid": task["uid"],
                "start_time": datetime.datetime.fromtimestamp(task["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": datetime.datetime.fromtimestamp(task["end_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                "node_name": self._node_name,
                "return_code": task["return_code"],
                "pid": task["pid"],
            })
            self.sql_conn.commit()
        except Exception as e:
            self.logger(f"Write task to sql failed, the exception is {e}, {task}", level="error")

    def _check_uid(self, uid: int) -> bool:
        sql = "select * from user where uid = %s"
        self.cursor.execute(sql, (uid,))
        return len(self.cursor.fetchall()) > 0

    def create_task(self, *requests: list[bytes]) -> list[bytes]:
        """Create a task."""
        requests = json.loads(requests[0])

        assert "task_name" in requests, "task_name is required"
        task_name = requests.get("task_name", None)
        assert isinstance(task_name, str), "task_name must be a string"

        assert "args" in requests, "args is required"
        args = requests.get("args", None)
        assert isinstance(args, list), "args must be a list"
        
        assert "uid" in requests, "uid is required"
        uid = requests.get("uid", None)
        assert isinstance(uid, int), "uid must be a int"
        assert self._check_uid(uid), "uid not exists"

        task_id = utils.get_uuid()
        stdout = requests.get("stdout", None)
        stderr = requests.get("stderr", None)
        if len(self._tasks) >= self.max_task_num:
            return [json.dumps({
                "status": 403,
                "msg": "task limit reached",
                "data": None,
            }).encode()]
        else:
            self._tasks[task_id] = {
                "task_name": task_name,
                "args": args,
                "uid": uid,
                "status": TaskWorker.TASK_STATUS.waiting,
                "start_time": time.time(),
                "end_time": None,
                "stdout": open(stdout, "w") if stdout else subprocess.PIPE,
                "stderr": open(stderr, "w") if stderr else subprocess.PIPE,
                "handler": None,
                "pid": None,
                "return_code": None,
            }

            return [json.dumps({
                "status": 200,
                "msg": "success",
                "data": {
                    "task_id": task_id,
                    "start_time": datetime.datetime.fromtimestamp(self._tasks[task_id]["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                }
            }).encode()]

    def kill_task(self, *requests: list[bytes]) -> list[bytes]:
        """Kill a task."""
        requests = json.loads(requests[0])
        assert "task_id" in requests, "task_id is required"
        task_id = requests["task_id"]
        assert isinstance(task_id, str), "task_id must be a str"
        task = self._tasks.get(task_id, None)
        if task is None:
            return [json.dumps({
                "status": 400,
                "msg": "task not found",
                "data": None,
            }).encode()]
        if (
            task["status"] == TaskWorker.TASK_STATUS.waiting or 
            task["status"] == TaskWorker.TASK_STATUS.finished
        ):
            next_status = TaskWorker.TASK_STATUS.finished
        elif (
            task["status"] == TaskWorker.TASK_STATUS.running or
            task["status"] == TaskWorker.TASK_STATUS.killing
        ):
            next_status = TaskWorker.TASK_STATUS.killing
        task["status"] = next_status
        return [json.dumps({
            "status": 200,
            "msg": "success",
            "data": None,
        }).encode()]

    def get_task_info(self, *requests: list[bytes]) -> list[bytes]:
        """Get task info."""
        requests = json.loads(requests[0])
        assert "task_id" in requests, "task_id is required"
        task_id = requests["task_id"]
        assert isinstance(task_id, str), "task_id must be a str"
        task = self._tasks.get(task_id, None)
        if task is None:
            return [json.dumps({
                "status": 400,
                "msg": "task not found",
                "data": None,
            }).encode()]
        else:
            return [json.dumps({
                "status": 200,
                "msg": "success",
                "data": {
                    "status": task["status"].name,
                    "start_time": datetime.datetime.fromtimestamp(task["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": None,
                    "pid": task["pid"],
                    "args": json.dumps(task["args"]),
                }
            }).encode()]

    def get_task_info_by_uid(self, *requests: list[bytes]) -> list[bytes]:
        """Get task info by uid."""
        requests = json.loads(requests[0])
        assert "uid" in requests, "uid is required"
        uid = requests["uid"]
        assert isinstance(uid, int), "uid must be a int"
        result = {}
        for task_id, task in self._tasks.items():
            if task["uid"] == uid:
                result[task_id] = {
                    "status": task["status"].name,
                    "start_time": datetime.datetime.fromtimestamp(task["start_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": None,
                    "pid": task["pid"],
                    "args": json.dumps(task["args"]),
                }
        
        return [json.dumps({
            "status": 200,
            "msg": "success",
            "data": result,
        }).encode()]

def create_worker(
    max_task_num: int,
    mysql_host: str,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
    mysql_charset: str,
    node_name: str,
    broker_addr: str,
    internal_addr: str,
    logger_addr: str,
    heartbeat_liveness: int = 5,
    heartbeat_interval: int = 2500,
    ctx: zmq.Context = None,
) -> WorkerNode:
    worker = TaskWorker(
        node_name=node_name,
        max_task_num=max_task_num,
        mysql_host=mysql_host,
        mysql_user=mysql_user,
        mysql_password=mysql_password,
        mysql_database=mysql_database,
        mysql_charset=mysql_charset
    )
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

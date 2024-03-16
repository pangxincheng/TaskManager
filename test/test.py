import argparse
import multiprocessing as mp
if mp.get_start_method(allow_none=True) is None:
    mp.set_start_method("spawn")

from task_manager.core.logger import LoggerNode
from task_manager.core.broker import BrokerNode
from task_manager.manager.gpu import create_worker as create_gpu_worker
from task_manager.manager.task import create_worker as create_task_worker
from task_manager.cli_controller import CliController

# logger
logger_node_name = "logger"
logger_addr = "tcp://127.0.0.1:5555"

# broker
broker_node_name = "api"
broker_external_addr = "tcp://127.0.0.1:5556"
broker_internal_addr = "inproc:///tmp/broker.sock"

# gpu
gpu_node_name = "localhost-gpu"
gpu_internal_addr = "inproc:///tmp/gpu.sock"

# task
task_node_name = "localhost-task"
task_external_addr = "tcp://127.0.0.1:5558"
task_internal_addr = "inproc:///tmp/task.sock"

def logger_fn(logger_node_name, logger_addr):
    logger = LoggerNode(
        node_name=logger_node_name,
        logger_addr=logger_addr
    )
    logger.run()

def broker_fn(broker_node_name, logger_addr, broker_external_addr, broker_internal_addr):
    broker = BrokerNode(
        node_name=broker_node_name, 
        logger_addr=logger_addr,
        external_addr=broker_external_addr,
        internal_addr=broker_internal_addr,
    )
    broker.run()

def gpu_fn(gpu_node_name, broker_external_addr, gpu_internal_addr, logger_addr):
    worker = create_gpu_worker(
        unit="MiB",
        chunk_size=512,
        node_name=gpu_node_name,
        broker_addr=broker_external_addr,
        internal_addr=gpu_internal_addr,
        logger_addr=logger_addr,
    )
    worker.run()

def task_fn(task_node_name, broker_external_addr, task_internal_addr, logger_addr, mysql_password):
    worker = create_task_worker(
        max_task_num=10,
        mysql_host="localhost",
        mysql_user="root",
        mysql_password=mysql_password,
        mysql_database="taskmanager",
        mysql_charset="utf8",
        node_name=task_node_name,
        broker_addr=broker_external_addr,
        internal_addr=task_internal_addr,
        logger_addr=logger_addr,
    )
    worker.run()

def cli_controller_fn(broker_addr, logger_addr):
    cli_controller = CliController(
        broker_addr=broker_external_addr,
        logger_addr=logger_addr,
    )
    cli_controller.cmdloop()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str, choices=["client", "server"])
    parser.add_argument("--mysql_password", type=str)
    return parser.parse_args()

def main():
    args = parse_args()
    if args.type == "server":
        logger_process = mp.Process(
            target=logger_fn,
            args=(
                logger_node_name, 
                logger_addr
            )
        )
        broker_process = mp.Process(
            target=broker_fn,
            args=(
                broker_node_name, 
                logger_addr, 
                broker_external_addr, 
                broker_internal_addr
            )
        )
        gpu_process = mp.Process(
            target=gpu_fn,
            args=(
                gpu_node_name, 
                broker_external_addr, 
                gpu_internal_addr, 
                logger_addr
            )
        )
        task_process = mp.Process(
            target=task_fn,
            args=(
                task_node_name, 
                broker_external_addr, 
                task_internal_addr, 
                logger_addr,
                args.mysql_password,
            )
        )
        logger_process.start()
        broker_process.start()
        gpu_process.start()
        task_process.start()
        logger_process.join()
        broker_process.join()
        gpu_process.join()
        task_process.join()
    else:
        cli_controller_fn(broker_external_addr, logger_addr)

if __name__ == "__main__":
    main()

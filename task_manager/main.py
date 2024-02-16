import argparse
import multiprocessing as mp

from task_manager.core.logger import LoggerNode
from task_manager.core.broker import BrokerNode
from task_manager.manager.gpu import GPUManager
from task_manager.manager.task import TaskManager
from task_manager.controller.cli_controller import CliController

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
task_node_name = "task"
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
    gpu = GPUManager(
        node_name=gpu_node_name,
        broker_addr=broker_external_addr,
        internal_addr=gpu_internal_addr,
        logger_addr=logger_addr,
    )
    gpu.run()

def task_fn(task_node_name, broker_external_addr, task_internal_addr, logger_addr):
    task = TaskManager(
        node_name=task_node_name,
        broker_addr=broker_external_addr,
        internal_addr=task_internal_addr,
        logger_addr=logger_addr,
    )
    task.run()

def cli_controller_fn(broker_addr, logger_addr):
    cli_controller = CliController(
        broker_addr=broker_external_addr,
        logger_addr=logger_addr,
    )
    cli_controller.cmdloop()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str, choices=["client", "server"])
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
                logger_addr
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

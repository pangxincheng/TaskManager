import rich

import zmq

import task_manager.core.const as const
from task_manager.core.logger import LoggerNode
from task_manager.core.broker import BrokerNode
from task_manager.manager.gpu import GPUManager
from task_manager.manager.task import TaskManager

def test():

    # logger
    logger_node_name = "logger"
    logger_addr = "tcp://127.0.0.1:5555"

    # broker
    broker_node_name = "api"
    broker_external_addr = "tcp://127.0.0.1:5556"
    broker_internal_addr = "inproc:///tmp/broker.sock"

    # gpu
    gpu_node_name = "localhost-gpu"
    gpu_external_addr = "tcp://127.0.0.1:5557"
    gpu_internal_addr = "inproc:///tmp/gpu.sock"

    # task
    task_node_name = "task"
    task_external_addr = "tcp://127.0.0.1:5558"
    task_internal_addr = "inproc:///tmp/task.sock"

    class TestClient:

        def __init__(self, tgt_addr: str) -> None:
            self.ctx: zmq.Context = zmq.Context()
            self.client: zmq.Socket = self.ctx.socket(zmq.DEALER)
            self.client.linger = 1
            self.client.connect(tgt_addr)

        def send(self, service_name: str, msg: list[bytes]) -> list[bytes]:
            self.client.send_multipart([
                const.EMPTY,
                const.CALL,
                const.EMPTY,
                service_name.encode(),
                *msg,
            ])
            return self.client.recv_multipart()
        
        def __del__(self):
            self.destroy()
        
        def destroy(self):
            self.client.close()
            self.ctx.term()

    def parse_args():
        import argparse
        parser = argparse.ArgumentParser(description="Test the communication between the broker, worker, and repeater.")
        parser.add_argument("--type", type=str)
        return parser.parse_args()

    def main():
        args = parse_args()
        if args.type == "logger":
            logger = LoggerNode(
                node_name="logger",
                logger_addr=logger_addr
            )
            logger.run()
        elif args.type == "broker":
            broker = BrokerNode(
                node_name=broker_node_name, 
                logger_addr=logger_addr,
                external_addr=broker_external_addr,
                internal_addr=broker_internal_addr,
            )
            broker.run()
        elif args.type == "client":
            client = TestClient(broker_external_addr)
            while True:
                service_name = input("Enter a service name: ")
                msg = input("Enter a message: ")
                if msg == "exit":
                    break
                rich.print(client.send(service_name, [msg.encode()]))
        elif args.type == "gpu":
            gpu = GPUManager(
                node_name=gpu_node_name,
                broker_addr=broker_external_addr,
                internal_addr=gpu_internal_addr,
                logger_addr=logger_addr,
            )
            gpu.run()
        elif args.type == "task":
            task = TaskManager(
                node_name=task_node_name,
                broker_addr=broker_external_addr,
                internal_addr=task_internal_addr,
                logger_addr=logger_addr,
            )
            task.run()
        elif args.type == "cli_controller":
            from controller.cli_controller import CliController
            cli_controller = CliController(
                broker_addr=broker_external_addr,
                logger_addr=logger_addr,
            )
            cli_controller.cmdloop()

    main()

if __name__ == "__main__":
    test()

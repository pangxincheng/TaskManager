import zmq
import rich
import task_manager.core.const as const
from task_manager.core.logger import LoggerNode
from task_manager.core.broker import BrokerNode
from task_manager.core.worker import WorkerNode
from task_manager.core.repeater import RepeaterNode

# logger
logger_node_name = "logger"
logger_addr = "tcp://127.0.0.1:5555"


# broker
broker_node_name = "api"
broker_external_addr = "tcp://127.0.0.1:5556"
broker_internal_addr = "inproc:///tmp/broker.sock"

# broker1
broker1_node_name = "gpu"
broker1_external_addr = "tcp://127.0.0.1:5557"
broker1_internal_addr = "inproc:///tmp/broker1.sock"

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
    elif args.type == "worker":
        worker = WorkerNode(
            node_name="worker",
            logger_addr=logger_addr,
            broker_addr=broker_external_addr,
            service_name="say_hello",
        )
        def say_hello(msg: list[bytes]) -> list[bytes]:
            worker.logger(["Received message:", msg])
            return [b"Hello, " + msg[0]]
        worker.run(say_hello)
    elif args.type == "client":
        client = TestClient(broker_external_addr)
        while True:
            service_name = input("Enter a service name: ")
            msg = input("Enter a message: ")
            if msg == "exit":
                break
            rich.print(client.send(service_name, [msg.encode()]))
    elif args.type == "broker1":
        broker1 = BrokerNode(
            node_name=broker1_node_name, 
            logger_addr=logger_addr,
            external_addr=broker1_external_addr,
            internal_addr=broker1_internal_addr,
        )
        broker1.run()
    elif args.type == "worker1":
        worker1 = WorkerNode(
            node_name="worker1",
            logger_addr=logger_addr,
            broker_addr=broker1_external_addr,
            service_name="say_gpu",
        )
        def say_gpu(msg: list[bytes]) -> list[bytes]:
            worker1.logger(["Received message:", msg])
            return [b"Hello GPU, " + msg[0]]
        worker1.run(say_gpu)
    elif args.type == "repeater":
        repeater = RepeaterNode(
            node0_name=broker_node_name,
            node0_addr=broker_external_addr,
            node1_name=broker1_node_name,
            node1_addr=broker1_external_addr,
            logger_addr=logger_addr,
        )
        repeater.run()

if __name__ == "__main__":
    main()

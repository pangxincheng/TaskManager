import cmd2
import json
import rich
import argparse

from task_manager.core.client import Client

class CliController(cmd2.Cmd):

    def __init__(
        self,
        broker_addr: str,
        logger_addr: str,
    ):
        super().__init__()
        self.prompt = "(🚀task_manager)> "
        self.broker_addr = broker_addr
        self.logger_addr = logger_addr
        self.client = Client("cli", self.broker_addr, self.logger_addr)

    def _parse_device_ids(self, device_ids: list[str]):
        node_to_device_ids = {}
        for device_id in device_ids:
            node, device_ids = device_id.split(":")
            node = f"{node}-gpu"
            if "," in device_ids:
                device_ids = device_ids.split(",")
                device_ids = [int(device_id) for device_id in device_ids]
            elif "-" in device_id:
                device_ids = device_ids.split("-")
                device_ids = list(range(int(device_ids[0]), int(device_ids[1])+1))
            else:
                device_ids = [int(device_ids)]
            if node not in node_to_device_ids:
                node_to_device_ids[node] = []
            node_to_device_ids[node].extend(device_ids)
        return node_to_device_ids

    @cmd2.with_argparser(cmd2.Cmd2ArgumentParser())
    def do_exit(self, args):
        """Exit the application."""
        return True
    
    ggi_parser = cmd2.Cmd2ArgumentParser()
    ggi_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    @cmd2.with_argparser(ggi_parser)
    def do_get_gpu_info(self, args):
        """Get GPU info."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/get_gpu_info".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids))
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(return_msg)
            
    awd_parser = cmd2.Cmd2ArgumentParser()
    awd_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    @cmd2.with_argparser(awd_parser)
    def do_add_watchdog(self, args):
        """Add watchdog."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/add_watchdog".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids))
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(msg)

    rwd_parser = cmd2.Cmd2ArgumentParser()
    rwd_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    @cmd2.with_argparser(rwd_parser)
    def do_remove_watchdog(self, args):
        """Remove watchdog."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/remove_watchdog".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids))
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(msg)

    am_parser = cmd2.Cmd2ArgumentParser()
    am_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    am_parser.add_argument("--mem_size", type=int)
    am_parser.add_argument("--unit", type=str, choices=["B", "KiB", "MiB", "GiB"])
    @cmd2.with_argparser(am_parser)
    def do_allocate_memory(self, args):
        """Allocate memory."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/allocate_memory".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids)),
                        "mem_sizes": [args.mem_size for _ in range(len(device_ids))],
                        "units": [args.unit for _ in range(len(device_ids))]
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(msg)

    pm_parser = cmd2.Cmd2ArgumentParser()
    pm_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    pm_parser.add_argument("--mem_size", type=int)
    pm_parser.add_argument("--unit", type=str, choices=["B", "KiB", "MiB", "GiB"])
    @cmd2.with_argparser(pm_parser)
    def do_preempt_memory(self, args):
        """Preempt memory."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/preempt_memory".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids)),
                        "mem_sizes": [args.mem_size for _ in range(len(device_ids))],
                        "units": [args.unit for _ in range(len(device_ids))]
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(msg)

    apm_parser = cmd2.Cmd2ArgumentParser()
    apm_parser.add_argument("--device_ids", type=str, nargs="+", help="eg: 127.0.0.1:0 or 127.0.0.1:0,2 or 127.0.0.1:0-3")
    apm_parser.add_argument("--mem_size", type=int)
    apm_parser.add_argument("--unit", type=str, choices=["B", "KiB", "MiB", "GiB"])
    @cmd2.with_argparser(apm_parser)
    def do_auto_preempt_memory(self, args):
        """Auto preempt memory."""
        node_to_device_ids = self._parse_device_ids(args.device_ids)
        return_msg = {}
        for node, device_ids in node_to_device_ids.items():
            msg = self.client.send(
                service_name=f"/{node}/auto_preempt_memory".encode(),
                request=[
                    json.dumps({
                        "device_ids": sorted(set(device_ids)),
                        "mem_sizes": [args.mem_size for _ in range(len(device_ids))],
                        "units": [args.unit for _ in range(len(device_ids))]
                    }).encode()
                ]
            )
            msg = json.loads(msg[0])
            return_msg[node.split("-")[0]] = msg
        rich.print(msg)

    ct_parser = cmd2.Cmd2ArgumentParser()
    ct_parser.add_argument("--stdout", type=str, default=None)
    ct_parser.add_argument("--stderr", type=str, default=None)
    ct_parser.add_argument('--task_name', type=str, required=True)
    ct_parser.add_argument('--uid', type=int, required=True)
    ct_parser.add_argument('args', nargs=argparse.REMAINDER)
    @cmd2.with_argparser(ct_parser)
    def do_create_task(self, args):
        """Create task."""
        msg = self.client.send(
            service_name="/localhost-task/create_task".encode(),
            request=[
                json.dumps({
                    "stdout": args.stdout,
                    "stderr": args.stderr,
                    "args": args.args,
                    "task_name": args.task_name,
                    "uid": args.uid,
                }).encode()
            ]
        )
        msg = json.loads(msg[0])
        rich.print(msg)
    
   
    kt_parser = cmd2.Cmd2ArgumentParser()
    kt_parser.add_argument("--task_ids", type=str, nargs="+")
    @cmd2.with_argparser(kt_parser)
    def do_kill_task(self, args):
        """Kill task."""
        msg = self.client.send(
            service_name="/localhost-task/kill_task".encode(),
            request=[
                json.dumps({
                    "task_ids": args.task_ids
                }).encode()
            ]
        )
        msg = json.loads(msg[0])
        rich.print(msg)

    gts_parser = cmd2.Cmd2ArgumentParser()
    gts_parser.add_argument("--task_ids", type=str, nargs="+")
    @cmd2.with_argparser(gts_parser)
    def do_get_task_info(self, args):
        """Get task info."""
        msg = self.client.send(
            service_name="/localhost-task/get_task_info".encode(),
            request=[
                json.dumps({
                    "task_ids": args.task_ids
                }).encode()
            ]
        )
        msg = json.loads(msg[0])
        rich.print(msg)

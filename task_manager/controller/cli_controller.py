import os
import cmd2
import time
import rich
import signal
import argparse

import task_manager.utils.zmq_utils as zmq_utils
import task_manager.utils.common_utils as common_utils

def exit(signum, frame):
    rich.print("=> [bold red]\[ERROR][/bold red] please use [italic blue]exit[/italic blue] to exit the cli controller🤗")
signal.signal(signal.SIGINT, exit)
signal.signal(signal.SIGTERM, exit)

class CLIController(cmd2.Cmd):

    def __init__(
        self,
        core_manager_addr: str,
        log_dir: str="logs",
        log_level: str="INFO",
    ):
        super().__init__()
        self.prompt = "(🚀task_manager)> "
        self.core_manager_addr = core_manager_addr
        self.log_dir = log_dir
        self.log_level = log_level
        self.logger = None
        self.client = None
        self.identity = "cli_controller"

        self._init_controller()

    def _init_controller(self):
        self.logger = common_utils.get_logger(
            logger_name="cli_controller",
            log_level=self.log_level,
            handler=os.path.join(self.log_dir, "cli_controller.log")
        )

        self.logger.info("init core client")
        self.client = zmq_utils.ZMQClient(
            addr=self.core_manager_addr,
            identity=self.identity
        )
        time.sleep(1)

    @cmd2.with_argparser(cmd2.Cmd2ArgumentParser())
    def do_exit(self, args):
        """Exit the application."""
        self.logger.info("=> [info] exit cli server...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "exit",
            "kwargs": {},
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)
        self.client.close()
        return True

    ggibi_parser = cmd2.Cmd2ArgumentParser()
    ggibi_parser.add_argument("--identities", type=str, nargs="+", default=[], help="identities")
    ggibi_parser.add_argument("--info_level", type=str, default="simple", help="simple or detail", choices=["simple", "detail"])
    @cmd2.with_argparser(ggibi_parser)
    def do_get_gpus_info_by_identities(self, args):
        """Get gpu information by identities."""
        self.logger.info("=> [info] get gpu information by identities...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "get_gpus_info_by_identities",
            "kwargs": {
                "identities": args.identities,
                "info_level": args.info_level
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    ggibdi_parser = cmd2.Cmd2ArgumentParser()
    ggibdi_parser.add_argument("--device_ids", type=int, nargs="+", default=[], help="device ids")
    ggibdi_parser.add_argument("--info_level", type=str, default="simple", help="simple or detail", choices=["simple", "detail"])
    @cmd2.with_argparser(ggibdi_parser)
    def do_get_gpus_info_by_device_ids(self, args):
        """Get gpu information."""
        self.logger.info("=> [info] get gpu information...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "get_gpus_info_by_device_ids",
            "kwargs": {
                "device_ids": args.device_ids,
                "info_level": args.info_level
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    start_wdbdi_parser = cmd2.Cmd2ArgumentParser()
    start_wdbdi_parser.add_argument("--device_ids", type=int, nargs="+", help="device ids")
    @cmd2.with_argparser(start_wdbdi_parser)
    def do_start_watch_dog_by_device_ids(self, args):
        """Start watch dog by device ids."""
        self.logger.info("=> [info] start watch dog by device ids...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "start_watch_dog_by_device_ids",
            "kwargs": {
                "device_ids": args.device_ids
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    stop_wdbi_parser = cmd2.Cmd2ArgumentParser()
    stop_wdbi_parser.add_argument("--identities", type=str, nargs="+", default=[], help="identities")
    @cmd2.with_argparser(stop_wdbi_parser)
    def do_stop_watch_dog_by_identities(self, args):
        """Stop watch dog by identities."""
        self.logger.info("=> [info] stop watch dog by identities...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "stop_watch_dog_by_identities",
            "kwargs": {
                "identities": args.identities
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    mabi_parser = cmd2.Cmd2ArgumentParser()
    mabi_parser.add_argument("--identities", type=str, nargs="+", help="device ids")
    mabi_parser.add_argument("--chunk_sizes", type=int, nargs="+", help="chun size")
    mabi_parser.add_argument("--max_sizes", type=int, nargs="+", help="max size")
    mabi_parser.add_argument("--units", type=str, nargs="+", help="unit", choices=["B", "KiB", "MiB", "GiB"])
    @cmd2.with_argparser(mabi_parser)
    def do_mem_alloc_by_identities(self, args):
        """Memory allocation by identities."""
        self.logger.info("=> [info] memory allocation by identities...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "mem_alloc_by_identities",
            "kwargs": {
                "identities": args.identities,
                "chunk_sizes": args.chunk_sizes,
                "max_sizes": args.max_sizes,
                "units": args.units,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    mrbi_parser = cmd2.Cmd2ArgumentParser()
    mrbi_parser.add_argument("--identities", type=str, nargs="+", help="device ids")
    mrbi_parser.add_argument("--mem_sizes", type=int, nargs="+", help="chun size")
    mrbi_parser.add_argument("--units", type=str, nargs="+", help="unit", choices=["B", "KiB", "MiB", "GiB"])
    @cmd2.with_argparser(mrbi_parser)
    def do_mem_release_by_identities(self, args):
        """Memory release by identities."""
        self.logger.info("=> [info] memory release by identities...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "mem_release_by_identities",
            "kwargs": {
                "identities": args.identities,
                "mem_sizes": args.mem_sizes,
                "units": args.units,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    start_pbi_parser = cmd2.Cmd2ArgumentParser()
    start_pbi_parser.add_argument("--identities", type=str, nargs="+", help="device ids")
    start_pbi_parser.add_argument("--chunk_sizes", type=int, nargs="+", help="chun size")
    start_pbi_parser.add_argument("--max_sizes", type=int, nargs="+", help="max size")
    start_pbi_parser.add_argument("--units", type=str, nargs="+", help="unit", choices=["B", "KiB", "MiB", "GiB"])
    start_pbi_parser.add_argument("--auto_close", action="store_true", help="auto close")
    @cmd2.with_argparser(start_pbi_parser)
    def do_start_preemptive_by_identities(self, args):
        """Start preemptive by identities."""
        self.logger.info("=> [info] start preemptive...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "start_preemptive_by_identities",
            "kwargs": {
                "identities": args.identities,
                "chunk_sizes": args.chunk_sizes,
                "max_sizes": args.max_sizes,
                "units": args.units,
                "auto_close": args.auto_close,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    stop_pbi_parser = cmd2.Cmd2ArgumentParser()
    stop_pbi_parser.add_argument("--identities", type=str, nargs="+", help="device ids")
    @cmd2.with_argparser(stop_pbi_parser)
    def do_stop_preemptive_by_identities(self, args):
        """Stop preemptive by identities."""
        self.logger.info("=> [info] stop preemptive...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "stop_preemptive_by_identities",
            "kwargs": {
                "identities": args.identities,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    at_parser = cmd2.Cmd2ArgumentParser()
    at_parser.add_argument("--stdout_file", type=str, completer=cmd2.Cmd.path_complete, required=True, help="stdout file")
    at_parser.add_argument("--stderr_file", type=str, completer=cmd2.Cmd.path_complete, required=True, help="stderr file")
    at_parser.add_argument("user_args", nargs=argparse.REMAINDER, completer=cmd2.Cmd.path_complete, help="user args")
    @cmd2.with_argparser(at_parser)
    def do_add_task(self, args):
        """Add task."""
        self.logger.info("=> [info] add task...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "add_task",
            "kwargs": {
                "user_args": args.user_args,
                "stdout_file": args.stdout_file,
                "stderr_file": args.stderr_file,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    rt_parser = cmd2.Cmd2ArgumentParser()
    rt_parser.add_argument("--identities", type=str, nargs="+", default=[], help="task id")
    @cmd2.with_argparser(rt_parser)
    def do_remove_tasks(self, args):
        """Remove tasks."""
        self.logger.info("=> [info] remove tasks...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "remove_tasks",
            "kwargs": {
                "identities": args.identities,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

    gtibi_parser = cmd2.Cmd2ArgumentParser()
    gtibi_parser.add_argument("--identities", type=str, nargs="+", default=[], help="task id")
    @cmd2.with_argparser(gtibi_parser)
    def do_get_task_info_by_identities(self, args):
        """Get task info by identities."""
        self.logger.info("=> [info] Get task info by identities...")
        self.client.send_binary(common_utils.dict_to_byte_msg({
            "function": "get_task_info_by_identities",
            "kwargs": {
                "identities": args.identities,
            },
        }))
        msg = common_utils.byte_msg_to_dict(self.client.recv_binary()[0])
        rich.print(msg)

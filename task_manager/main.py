import os
import sys
import rich
import time
import argparse
import multiprocessing as mp

if mp.get_start_method(allow_none=True) is None:
    mp.set_start_method("spawn")
else:
    assert mp.get_start_method() == "spawn", "Only support spawn start method"

import task_manager.utils.common_utils as common_utils
from task_manager.manager.core import CoreManager
from task_manager.controller.cli_controller import CLIController

def parse_args():
    identity_id = common_utils.md5(str(time.time()))
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_dir", default="logs", help="Log dir")
    parser.add_argument("--log_level", default="INFO", help="Log level")
    parser.add_argument("--web_controller", action="store_true", help="Whether start web gui to watch GPU usage&Tasks")
    parser.add_argument(
        "--core_manager_addr", 
        type=str, 
        default=f"ipc:///tmp/core_manager-{identity_id}.sock", 
        help="Address to run Core manager on"
    )
    parser.add_argument(
        "--gpu_manager_addr",
        type=str,
        default=f"ipc:///tmp/gpu_manager-{identity_id}.sock",
        help="Address to run GPU manager on"
    )
    parser.add_argument(
        "--task_manager_addr",
        type=str,
        default=f"ipc:///tmp/task_manager-{identity_id}.sock",
        help="Address to run Task manager on"
    )
    args = parser.parse_args()
    os.makedirs(args.log_dir, exist_ok=True)
    sys.argv = sys.argv[:1]
    return args

def start_core_manager(args):
    core_manager = CoreManager(
        core_manager_addr=args.core_manager_addr,
        gpu_manager_addr=args.gpu_manager_addr,
        task_manager_addr=args.task_manager_addr,
        log_dir=args.log_dir,
        log_level=args.log_level,
    )
    core_manager.start()
    time.sleep(1)
    return core_manager

def start_cli_controller(args):
    cli_controller = CLIController(
        core_manager_addr=args.core_manager_addr,
        log_dir=args.log_dir,
        log_level=args.log_level,
    )
    cli_controller.cmdloop()

def main():
    args = parse_args()
    rich.print("🚀[bold green]Task Manager[/bold green]")
    rich.print("[bold green]Args[/bold green]")
    rich.print(vars(args))
    rich.print("=> [bold green]\[INFO][/bold green] Start Core Manager...")
    start_core_manager(args)

    if args.web_controller:
        rich.print("=> [bold green]\[INFO][/bold green] Start Web Controller...")
        pass

    rich.print("=> [bold green]\[INFO][/bold green] Start CLI Controller...")
    start_cli_controller(args)

if __name__ == "__main__":
    main()

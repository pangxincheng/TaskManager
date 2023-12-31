# Task Manager
## Description
When using a GPU cluster, you may encounter the following problems:
1. You can only submit one task at a time, but the GPU allocated to you can support running multiple tasks (for example, your task only occupies half of the GPU memory).
2. The submitted task has been in the queue for a long time, but due to some small details, you want to modify the parameters without re-queuing.

This project is designed to solve these two problems.

Essentially, it is a task manager that allows you to efficiently utilize the resources allocated to you by the GPU cluster without breaking the rules.

## Installation
```shell
git clone https://github.com/pangxincheng/TaskManager
cd TaskManager
# activate your virtual environment
pip install -r requirements.txt
pip install -e .
```

## Usage
### CLI Controller
```shell
python -m task_manager.main
```
It will start a CLI program (enter `help` for more prompts), through which you can manage your tasks and GPUs.

## Acknowledgements
This repository uses the following libraries:

- [cmd2](https://github.com/python-cmd2/cmd2): A tool for building interactive command line applications in Python
- [rich](https://github.com/Textualize/rich): A Python library for rich text and beautiful formatting in the terminal
- [pycuda](https://github.com/inducer/pycuda): A library that lets you access Nvidia's CUDA parallel computation API from Python
- [pyzmq](https://github.com/zeromq/pyzmq): Python bindings for ØMQ (a lightweight and fast messaging implementation)
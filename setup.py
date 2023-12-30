from setuptools import setup, find_packages
print(find_packages())
setup(
    name="task_manager",
    version="0.1.0",
    description="A simple tool to manage GPU resources",
    author="Xincheng Pang",
    author_email="pangxincheng@foxmail.com",
    packages=[package for package in find_packages()],
    requires=[
        "pycuda",
        "pyzmq",
        "rich",
        "cmd2",
        "numpy"
    ]
)
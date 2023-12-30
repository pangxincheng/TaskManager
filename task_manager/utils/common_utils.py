import sys
import json
import logging
import hashlib
import warnings
from typing import Optional, Callable

class Register:
    def __init__(self, registry_name) -> None:
        self._dict = {}
        self._name = registry_name

    def __setitem__(self, key, value) -> None:
        if not callable(value):
            raise Exception(f"Value of a Registry must be a callable!\nValue: {value}")
        if key is None:
            key = value.__name__
        if key in self._dict:
            warnings.warn("Key %s already in registry %s." % (key, self._name))
        self._dict[key] = value

    def register(self, target) -> Callable:
        """Decorator to register a function or class."""

        def add(key, value):
            self[key] = value
            return value

        if callable(target):
            # @reg.register
            return add(None, target)
        # @reg.register('alias')
        return lambda x: add(target, x)
    
    def get(self, key, default=None) -> Optional[Callable]:
        return self._dict.get(key, default)

    def __getitem__(self, key) -> Callable:
        return self._dict[key]

    def __contains__(self, key) -> bool:
        return key in self._dict

    def keys(self):
        """key"""
        return self._dict.keys()

def byte_msg_to_dict(msg: bytes) -> dict:
    return json.loads(msg.decode("utf-8"))

def dict_to_byte_msg(d: dict) -> bytes:
    return json.dumps(d).encode("utf-8")

def to_bytes(sz: int, unit: str) -> int:
    if unit == "B": return sz
    elif unit == "KiB": return sz * 1024
    elif unit == "MiB": return sz * 1024 * 1024
    elif unit == "GiB": return sz * 1024 * 1024 * 1024
    else: raise Exception(f"unknown unit {unit}, support: [B, KiB, MiB, GiB]")

def fmt_bytes(sz: int) -> str:
    if sz < 1024: return f"{sz} B"
    elif sz < 1024 * 1024: return f"{sz / 1024} KiB"
    elif sz < 1024 * 1024 * 1024: return f"{sz / 1024 / 1024} MiB"
    else: return f"{sz / 1024 / 1024 / 1024} GiB"

def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()

def get_logger(
    logger_name: Optional[str] = None,
    log_level: str = "INFO",
    handler: Optional[str] = None,
    fmt_str: str = "[%(asctime)s] [%(levelname)s] %(message)s",
) -> logging.Logger:
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(handler)
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    formatter = logging.Formatter(fmt_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

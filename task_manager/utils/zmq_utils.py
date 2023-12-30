import zmq
import time
from typing import Union, List, Optional

import task_manager.utils.common_utils as common_utils

class ZMQClient:

    def __init__(
        self,
        addr: str,
        identity: Optional[str]=None,
        context: Optional[zmq.Context]=None
    ) -> None:
        self._addr = addr
        self._identity = common_utils.md5(str(time.time())) if identity is None else identity
        self._context = zmq.Context() if context is None else context
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt_string(zmq.IDENTITY, self._identity)
        self._socket.connect(self._addr)

    def send_binary(self, any: Union[bytes, str]) -> None:
        if type(any) == str:
            any = any.encode("utf-8")
        assert type(any) == bytes
        self._socket.send_multipart([any])

    def recv_binary(self) -> List[bytes]:
        return self._socket.recv_multipart()
    
    def close(self) -> None:
        self._socket.close()
    
    @property
    def addr(self) -> str:
        return self._addr

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def context(self) -> zmq.Context:
        return self._context
    
    @property
    def socket(self) -> zmq.Socket:
        return self._socket
    
class ZMQServer:

    def __init__(
        self,
        addr: str,
        context: Optional[zmq.Context]=None
    ) -> None:
        self._addr = addr
        self._context = zmq.Context() if context is None else context
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.bind(self._addr)

    def send_binary(self, any: Union[bytes, str], identity: Union[bytes, str]) -> None:
        if type(any) == str:
            any = any.encode("utf-8")
        if type(identity) == str:
            identity = identity.encode("utf-8")
        assert type(any) == bytes and type(identity) == bytes
        self._socket.send_multipart([identity, any])

    def recv_binary(self) -> List[bytes]:
        return self._socket.recv_multipart()
    
    def close(self) -> None:
        self._socket.close()
    
    @property
    def addr(self) -> str:
        return self._addr

    @property
    def context(self) -> zmq.Context:
        return self._context
    
    @property
    def socket(self) -> zmq.Socket:
        return self._socket
import logging
import socket
import time
from typing import Any

from xqdb._conversion import from_arrow_results, to_arrow_inputs, validate_backend
from xqdb.xqdb import XqdbConnector, XqdbIOError

logger = logging.getLogger("xqdb")


def _convert_args(args: tuple[object, ...]) -> tuple[object, ...]:
    if len(args) > 8:
        raise TypeError("q functions accept at most 8 arguments")
    return tuple(to_arrow_inputs(arg) for arg in args)


# Q use IPC protocol version 6, which is compatible with kdb+
class Q(object):
    def __init__(
        self,
        host: str,
        port: int,
        user: str = "",
        passwd: str = "",
        enable_tls: bool = False,
        retries: int = 0,
        timeout: int = 0,
        backend: str = "pyarrow",
        symbol_encoding: str = "strict",
    ):
        if (not host) or host == socket.gethostname():
            host = "127.0.0.1"
        self.host = host
        self.port = port
        self.user = user
        self.retries = retries
        self.backend = backend
        self.q = XqdbConnector(host, port, user, passwd, enable_tls, timeout, 6)
        self.symbol_encoding = symbol_encoding

    @property
    def backend(self) -> str:
        return self._backend

    @backend.setter
    def backend(self, backend: str) -> None:
        self._backend = validate_backend(backend)

    @property
    def symbol_encoding(self) -> str:
        """How q text that is not valid UTF-8 is decoded: ``"strict"`` fails the
        response, ``"lossy"`` replaces each invalid sequence with U+FFFD."""
        return self.q.symbol_encoding

    @symbol_encoding.setter
    def symbol_encoding(self, symbol_encoding: str) -> None:
        self.q.symbol_encoding = symbol_encoding

    def connect(self):
        self.q.connect()

    def disconnect(self):
        self.q.shutdown()

    def sync(self, expr: str, *args) -> Any:
        args = _convert_args(args)
        if self.retries <= 0:
            return from_arrow_results(self.q.sync(expr, *args), self.backend)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return from_arrow_results(self.q.sync(expr, *args), self.backend)
                except XqdbIOError as e:
                    logging.info(
                        "Failed to sync - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def asyn(self, expr: str, *args):
        args = _convert_args(args)
        if self.retries <= 0:
            return self.q.asyn(expr, *args)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.q.asyn(expr, *args)
                except XqdbIOError as e:
                    logging.info(
                        "Failed to async - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def receive(self) -> Any:
        if self.retries <= 0:
            return from_arrow_results(self.q.receive(), self.backend)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return from_arrow_results(self.q.receive(), self.backend)
                except XqdbIOError as e:
                    logging.info(
                        "Failed to receive - '%s', retrying in %s seconds", e, 2**n
                    )
                    self.connect()
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

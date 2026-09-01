from typing import Any, Literal

from xqdb._conversion import from_arrow_results, to_arrow_inputs
from xqdb.xqdb import generate_j6_ipc_msg, read_j6_binary_table

_MSG_TYPES = {"async": 0, "sync": 1, "response": 2}


def read_binary6(
    filepath: str, backend: str = "pyarrow", symbol_encoding: str = "strict"
) -> Any:
    return from_arrow_results(
        read_j6_binary_table(filepath, symbol_encoding=symbol_encoding), backend
    )


def serialize_as_ipc_bytes6(
    msg_type: Literal["async", "sync", "response"],
    enable_compression: bool,
    any: object,
) -> bytes:
    try:
        wire_type = _MSG_TYPES[msg_type]
    except KeyError:
        raise ValueError(
            f"expected 'async', 'sync', or 'response' msg_type, got {msg_type!r}"
        ) from None
    return generate_j6_ipc_msg(wire_type, enable_compression, to_arrow_inputs(any))


__all__ = ["read_binary6", "serialize_as_ipc_bytes6"]

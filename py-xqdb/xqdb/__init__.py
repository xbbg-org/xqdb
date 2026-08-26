from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:

    class XqdbQOperator:
        PLUS: ClassVar["XqdbQOperator"]

        def __init__(self, name: str) -> None: ...

        @property
        def name(self) -> str: ...

    class XqdbQLambda:
        def __init__(self, source: str, context: str = "") -> None: ...

        @property
        def source(self) -> str: ...

        @property
        def context(self) -> str: ...
else:
    from xqdb.xqdb import XqdbQLambda, XqdbQOperator

from xqdb.exceptions import XqdbAuthError, XqdbError, XqdbIOError
from xqdb.q import Q
from xqdb.util import read_binary6, serialize_as_ipc_bytes6

__all__ = [
    "Q",
    "XqdbAuthError",
    "XqdbError",
    "XqdbIOError",
    "XqdbQLambda",
    "XqdbQOperator",
    "read_binary6",
    "serialize_as_ipc_bytes6",
]

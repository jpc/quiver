"""pupyarrow — pure-Python Arrow IPC reader/writer. numpy-only."""
from .reader import (ArrowType, BytesReader, FeatherFile, Field,  # noqa
                     LazyBuffer, Schema)
from .writer import StreamReader, StreamWriter, write_feather  # noqa
from . import fb  # noqa

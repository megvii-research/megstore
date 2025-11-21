from functools import partial
from typing import IO

import msgpack_numpy as _msgpack
from msgpack import OutOfData, UnpackValueError
from msgpack import version as _msgpack_version

__all__ = [
    "DEFAULT_MAX_BUFFER_SIZE",
    "OutOfData",
    "UnpackValueError",
    "Packer",
    "Unpacker",
    "pack",
    "packb",
    "unpack",
    "unpackb",
]

DEFAULT_PACK_OPTIONS = {
    "use_bin_type": True,
    "strict_types": False,
}

# raw: Default True for backward compatibility, will default to False in the
#   future, should always explicitly specify this parameter,
#   When False, will decode string bytes as UTF8
# max_buffer_size: Should always explicitly specify this parameter when processing
#   data over 100MB,
#   Limits buffer size when decoding from msgpack, if decoded object size
#   exceeds this value, raises BufferFull exception.
#   Defaults to 0, means system INT_MAX (claimed in documentation), actual
#   source code writes 2**31 - 1.
#   Buffer size set too large during msgpack decoding also get error,
#   exceeding 2**31 - 1 raises OverflowError exception.
# use_list: Default True, msgpack array decodes to list, otherwise to tuple
# strict_map_key: Default True, msgpack dict does not allow types other than
#   str or bytes as key
DEFAULT_UNPACK_OPTIONS = {
    "raw": False,
    "use_list": True,
}

if _msgpack_version >= (1, 0, 0):
    DEFAULT_UNPACK_OPTIONS["strict_map_key"] = False

DEFAULT_MAX_BUFFER_SIZE = 100 * 2**20  # 100MB


class Packer(_msgpack.Packer):
    def __init__(self, **kwargs):
        options = DEFAULT_PACK_OPTIONS.copy()
        options.update(kwargs)
        super().__init__(**options)


class Unpacker(_msgpack.Unpacker):
    def __init__(
        self, file_object: IO[bytes], max_buffer_size=DEFAULT_MAX_BUFFER_SIZE, **kwargs
    ):
        options = DEFAULT_UNPACK_OPTIONS.copy()
        options.update(kwargs)
        options["max_buffer_size"] = max_buffer_size
        super().__init__(file_object, **options)
        self.file_object = file_object
        self.options = options


pack = partial(_msgpack.pack, **DEFAULT_PACK_OPTIONS)
packb = partial(_msgpack.packb, **DEFAULT_PACK_OPTIONS)
unpack = partial(_msgpack.unpack, **DEFAULT_UNPACK_OPTIONS)
unpackb = partial(_msgpack.unpackb, **DEFAULT_UNPACK_OPTIONS)

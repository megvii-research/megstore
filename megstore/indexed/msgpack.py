import os
import struct
from array import array
from logging import getLogger as get_logger
from typing import Any, BinaryIO, Callable, Iterator, Optional, Union

from megfile import smart_exists
from megfile.utils import get_content_size, is_seekable, shadow_copy

from megstore.indexed.base import (
    INDEX_FILE_FORMAT,
    INDEX_FILE_HEADER_FORMAT,
    INDEX_FILE_POSTFIX,
    BaseIndexedReader,
    Countable,
    IndexHandlerReader,
    IndexHandlerWriter,
)
from megstore.interface import Appendable, BaseWriter, OpenBinaryIO, T
from megstore.utils import full_error_message, smart_limited_seekable_open

try:
    from megstore.utils import compat_msgpack
except ImportError:
    compat_msgpack = None

__all__ = [
    "IndexedMsgpackReader",
    "IndexedMsgpackWriter",
    "indexed_msgpack_open",
]

logger = get_logger(__name__)


MSGPACK_ARRAY32_FLAG = b"\xdd"
MSGPACK_ARRAY32_FLAG_SIZE = len(MSGPACK_ARRAY32_FLAG)
MSGPACK_ARRAY32_LENGTH_FORMAT = ">i"
MSGPACK_ARRAY32_LENGTH_SIZE = struct.calcsize(MSGPACK_ARRAY32_LENGTH_FORMAT)
MSGPACK_ARRAY32_HEADER_SIZE = MSGPACK_ARRAY32_FLAG_SIZE + MSGPACK_ARRAY32_LENGTH_SIZE


def _ensure_compat_msgpack():
    if compat_msgpack is None:
        raise ImportError(
            "`msgpack` is required to use megstore.indexed.msgpack, "
            "please install it with `pip install 'megstore[msgpack]'`"
        )


class IndexedMsgpackReader(BaseIndexedReader[T]):  # pytype: disable=not-indexable
    """Random reading for msgpack files

    If fd_idx is ``None``, attempt to build an index from fp_msgpack to
    support random reading
    """

    def __init__(self, *args, **kwargs):
        _ensure_compat_msgpack()

        super().__init__(*args, **kwargs)

    def _read_array_header(
        self,
        unpacker: "compat_msgpack.Unpacker",  # pytype: disable=attribute-error
    ) -> int:
        try:
            return unpacker.read_array_header()
        except compat_msgpack.OutOfData:
            return 0
        except compat_msgpack.UnpackValueError as error:
            # Assuming the msgpack file contains msgpack arrays,
            # raise this exception when reading non-array header
            raise ValueError(
                "invalid msgpack array header: %r, because of %s"
                % (self.name, full_error_message(error))
            )

    @classmethod
    def _build_index(
        cls,
        fp_data: BinaryIO,
        offsets: Union[IndexHandlerReader, array],
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ) -> int:
        """Build index from msgpack stream

        :raises ValueError: Cannot read valid msgpack array header from msgpack stream
        :returns: the latest offset
        """
        unpacker = cls._get_msgpack_unpacker(fp_data)
        cls._read_array_header(fp_data, unpacker)  # pytype: disable=wrong-arg-count

        # After reading array header, unpacker is at the start byte of the first value
        current_offset = unpacker.tell()
        try:
            while True:
                # Skip once first to ensure the next value can be read normally,
                # then append
                # Otherwise if ``OutOfData`` exception occurs meaning end of data
                # reached
                if index_build_callback:
                    index_build_callback(unpacker.unpack())
                else:
                    unpacker.skip()
                offsets.append(current_offset)
                current_offset = unpacker.tell()
        except compat_msgpack.OutOfData:
            # Record length exceeds actual length, only return index of
            # actually read values, and the last tell() of unpacker
            pass
        return current_offset

    def _get(self, index: int) -> T:  # pytype: disable=invalid-annotation
        """Read a value by specified index

        :param index: Target value's index
        :returns: The read value
        """
        offset = self._offsets[index]
        if index == self._count - 1:
            # Index is the last offset
            size = 0
        else:
            next_offset = self._offsets[index + 1]
            size = next_offset - offset
        self._file_object.seek(offset)

        unpacker = self._get_msgpack_unpacker(self._file_object, max_buffer_size=size)
        try:
            value = unpacker.unpack()
        except compat_msgpack.OutOfData:
            raise ValueError(
                "out of data: %r, index: %d, offset: %d" % (self.name, index, offset)
            )
        return value

    def _batch_get(self, index_slice: slice) -> Iterator[T]:
        """Read values with indices in the range [start_index, end_index) in one seek,
        sequentially

        :param start_index: Starting index
        :param end_index: Ending index
        :returns: List of read values
        """
        start_index, end_index, step = index_slice.indices(self.count())
        if step != 1 or start_index >= end_index:
            yield from super()._batch_get(index_slice)
            return

        start_offset = self._offsets[start_index]
        end_offset = -1
        if end_index >= self._count:
            size = 0
        else:
            end_offset = self._offsets[end_index]
            size = end_offset - start_offset
        file_object = shadow_copy(self._file_object)
        file_object.seek(start_offset)
        unpacker = self._get_msgpack_unpacker(file_object, max_buffer_size=size)
        index = start_index
        try:
            for index in range(start_index, end_index):
                yield unpacker.unpack()
        except compat_msgpack.OutOfData:
            raise ValueError(
                "out of data: %r, index: %d, offset: %d ~ %d"
                % (self.name, index, start_offset, end_offset)
            )

    @classmethod
    def _get_msgpack_unpacker(
        cls, file_object: BinaryIO, max_buffer_size: Optional[int] = None
    ) -> "compat_msgpack.Unpacker":  # pytype: disable=attribute-error
        """Get an Unpacker object created with the current instance's msgpack stream

        :param max_buffer_size: Maximum buffer size of Unpacker object (bytes),
            default None, if specified, the number should be **> 0**
        :returns: Unpacker object
        """
        if max_buffer_size is None:
            max_buffer_size = compat_msgpack.DEFAULT_MAX_BUFFER_SIZE
        if max_buffer_size > compat_msgpack.DEFAULT_MAX_BUFFER_SIZE:
            max_buffer_size = compat_msgpack.DEFAULT_MAX_BUFFER_SIZE
        unpacker = compat_msgpack.Unpacker(file_object, max_buffer_size=max_buffer_size)
        return unpacker


class IndexedMsgpackWriter(BaseWriter[T], Countable):  # pytype: disable=not-indexable
    """Used to write msgpack streams with index support"""

    def __init__(
        self,
        fp_msgpack: BinaryIO,
        fp_index_path: str,
        *,
        append_mode: bool = False,
        close_fileobj_when_close: bool = False,
    ):
        """
        :param fp_msgpack: msgpack stream, needs to be seekable,
            if it's an s3 address,
            can use ``megstore.utils.smart_limited_seekable_open`` to open
        :param fp_index: msgpack index stream
        :param close_fileobj_when_close: When ``True``, will close the stream on exit;
            when ``False``, will not close the stream
        """
        _ensure_compat_msgpack()

        super().__init__(
            fp_msgpack,
            append_mode=append_mode,
            close_fileobj_when_close=close_fileobj_when_close,
        )
        self._packer = compat_msgpack.Packer()
        self._count = self._read_array_header()

        mode = "wb"
        if append_mode is True:
            mode = "ab"
        fp_index = smart_limited_seekable_open(fp_index_path, mode=mode)
        self._fp_index_path = fp_index_path
        self._offsets = IndexHandlerWriter(
            fp_index,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
            append_mode=append_mode,
        )
        if fp_index.tell() == 0:
            self._offsets.write_header(intrusive=True)

    @property
    def _count(self) -> int:
        return getattr(self, "__count__", 0)

    @_count.setter
    def _count(self, value: int):
        setattr(self, "__count__", value)

    def count(self) -> int:
        return self._count

    def tell(self) -> int:
        return self.count()

    def _write_array_header(self, count: int):
        # Write array32 header + placeholder
        header_bytes = MSGPACK_ARRAY32_FLAG + struct.pack(
            MSGPACK_ARRAY32_LENGTH_FORMAT, count
        )
        self._file_object.write(header_bytes)

    def _read_array_header(self) -> int:
        if (
            not is_seekable(self._file_object)
            or get_content_size(self._file_object) == 0
        ):
            self._write_array_header(0)
            return 0

        try:
            first_byte = self._file_object.read(MSGPACK_ARRAY32_FLAG_SIZE)
            assert first_byte == MSGPACK_ARRAY32_FLAG, repr(first_byte)
            size = struct.unpack(
                MSGPACK_ARRAY32_LENGTH_FORMAT,
                self._file_object.read(MSGPACK_ARRAY32_LENGTH_SIZE),
            )[0]
            self._file_object.seek(0, os.SEEK_END)
            return size
        except Exception as error:
            # Assuming the msgpack file contains msgpack arrays,
            # raise this exception when reading non-array header
            raise ValueError(
                "invalid msgpack array header: %r, because of %s"
                % (self.name, full_error_message(error))
            )

    def append(self, value: T):
        offset = self._file_object.tell()

        value_bytes = self._packer.pack(value)
        self._file_object.write(value_bytes)

        self._offsets.append(offset)
        self._count += 1

    def commit(self):
        """Write already added values to msgpack stream"""
        self._file_object.seek(0)
        self._write_array_header(self._count)
        self._file_object.seek(0, os.SEEK_END)
        self._file_object.flush()

        self._offsets.commit()

    def _close(self):
        """Ensure data is written to stream and attempt to close stream"""
        size = get_content_size(self._file_object)

        self._file_object.seek(0)
        self._write_array_header(self._count)

        # re-seek to tail
        self._file_object.seek(0, os.SEEK_END)

        if self._append_mode is False:
            self._offsets.write_header(size=size)
            self._offsets.close()
        else:
            self._offsets.close()
            with smart_limited_seekable_open(
                self._fp_index_path, mode="rb+"
            ) as fp_index:
                IndexHandlerWriter(
                    fp_index,
                    typecode=INDEX_FILE_FORMAT,
                    header=INDEX_FILE_HEADER_FORMAT,
                ).write_header(size=size)

        if self._close_fileobj_when_close:
            self._file_object.close()


class IndexedMsgpackHandler(IndexedMsgpackReader, Appendable[T]):
    def __init__(
        self,
        fp_msgpack: BinaryIO,
        fp_index_path: Optional[str] = None,
        *,
        append_mode: bool = False,
        close_fileobj_when_close: bool = False,
    ):
        """
        :param fp_msgpack: msgpack data stream, if the data in the stream is
            not msgpack array, and no index stream is specified, building index
            from msgpack stream will fail, if it's an s3 address, can use
            megfile.s3_cached_open to open
        :param fp_index: Corresponding index data stream, default is None
        :param close_fileobj_when_close: When ``True``, close the stream on exit,
            default is ``False``
        """
        _ensure_compat_msgpack()

        super().__init__(
            fp_msgpack,
            fp_index_path,
            close_fileobj_when_close=close_fileobj_when_close,
            index_file_mode="rb+",
        )
        self._append_mode = append_mode

        if self._last_offset is None and len(self._offsets) > 0:
            # offsets read from index
            last_offset = self._offsets[-1]
            self._file_object.seek(last_offset)
            unpacker = self._get_msgpack_unpacker(fp_msgpack)
            try:
                unpacker.skip()
                self._last_offset = last_offset + unpacker.tell()
            except compat_msgpack.OutOfData:
                pass
        elif self._last_offset in (None, 0):
            # Index or msgpack is empty file stream
            self._last_offset = MSGPACK_ARRAY32_HEADER_SIZE
            self._write_array_header(0)

        self._file_object.seek(0, os.SEEK_END)
        if self._last_offset != self._file_object.tell():
            # Ensure no extra data at the end of msgpack, appending is safe
            raise ValueError(
                "offset mismatch: %r, expected: %d, got: %d"
                % (self.name, self._last_offset, self._file_object.tell())
            )
        if self._count != len(self._offsets):
            # Ensure msgpack header and index have consistent length
            raise ValueError(
                "msgpack length mismatch: %r, header: %d, index: %d"
                % (self.name, self._count, len(self._offsets))
            )
        self._packer = compat_msgpack.Packer(use_bin_type=True, strict_types=False)

    @property
    def mode(self) -> str:
        if self._append_mode:
            return "a+"
        return "w+"

    def _write_array_header(self, count: int):
        # Write array32 header + placeholder
        header_bytes = MSGPACK_ARRAY32_FLAG + struct.pack(
            MSGPACK_ARRAY32_LENGTH_FORMAT, count
        )
        self._file_object.write(header_bytes)

    def append(self, value: T):  # pytype: disable=invalid-annotation
        """Add a record

        :param value: Record to be added
        """
        if self._last_offset != self._file_object.tell():
            self._file_object.seek(0, os.SEEK_END)
        value_bytes = self._packer.pack(value)
        self._file_object.write(value_bytes)
        self._offsets.append(self._last_offset)
        self._last_offset = self._file_object.tell()
        self._count += 1

    def commit(self):
        """Write already added values to msgpack stream"""
        self._file_object.seek(0)
        self._write_array_header(self._count)

        # re-seek to tail
        self._file_object.seek(0, os.SEEK_END)
        self._file_object.flush()
        if isinstance(self._offsets, IndexHandlerReader):
            self._offsets.close()

    def _close(self):
        """Ensure data is written to stream and attempt to close stream"""
        self.commit()
        super()._close()


def indexed_msgpack_open(
    path: str,
    mode: str = "r",
    *,
    index_path: Optional[str] = None,
    open_func: Optional[OpenBinaryIO] = None,
) -> Union[IndexedMsgpackReader, IndexedMsgpackWriter, IndexedMsgpackHandler]:
    """Open an indexed msgpack file

    .. note::
        Currently ``indexed_msgpack_open`` does not support opening an ``s3_url`` in
        ``a`` mode, users need to manually pass an open_func that supports ``rb+`` mode,
        such as ``megfile.s3_cached_open`` / ``megfile.s3_memory_open`` provided by
        ``megfile``.

    :param path: msgpack file path
    :param mode: Opening mode, supports read ``r``, write ``w``, append ``a``
        and read-write ``w+`` / ``a+``, default ``r``
    :param index_path: Index file path, default is ``None``
    :param open_func: Open function for msgpack file stream
        Default uses ``smart_open`` with ``limited_seekable=True``
    :raises ValueError: Invalid mode
    :returns: Returns ``megstore.IndexedMsgpackReader`` when mode is ``r``,
        returns ``megstore.IndexedMsgpackWriter`` when mode is ``w``
    """
    _ensure_compat_msgpack()

    if mode not in ("r", "w", "a", "w+", "a+"):
        raise ValueError("unacceptable mode: %r" % mode)

    if open_func is None:
        open_func = smart_limited_seekable_open

    fp_mode = mode + "b"
    if mode in ("a", "a+") and smart_exists(path):
        fp_mode = "rb+"
    elif mode == "a":
        fp_mode = "wb"
    elif mode in ("w+", "a+"):
        fp_mode = "wb+"

    fp_msgpack_file = open_func(path, fp_mode)

    if index_path is None:
        index_path = path + INDEX_FILE_POSTFIX

    if mode == "r":
        return IndexedMsgpackReader(
            fp_msgpack_file, index_path, close_fileobj_when_close=True
        )

    append_mode = mode in ("a", "a+")
    if mode in ("w", "a"):
        return IndexedMsgpackWriter(
            fp_msgpack_file,
            index_path,
            append_mode=append_mode,
            close_fileobj_when_close=True,
        )
    return IndexedMsgpackHandler(
        fp_msgpack_file,
        index_path,
        append_mode=append_mode,
        close_fileobj_when_close=True,
    )

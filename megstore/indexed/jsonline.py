from array import array
from typing import Any, BinaryIO, Callable, Iterator, Optional, Union

from megfile import smart_open
from megfile.utils import shadow_copy

import megstore.utils.compat_json as json
from megstore.errors import InvalidJsonError
from megstore.indexed.base import (
    INDEX_FILE_POSTFIX,
    BaseIndexedReader,
    BaseIndexedWriter,
    IndexHandlerReader,
    OpenBinaryIO,
)
from megstore.interface import T

__all__ = [
    "IndexedJsonlineReader",
    "IndexedJsonlineWriter",
    "indexed_jsonline_open",
]

NEWLINE = b"\n"


def short_bytes(data: bytes, length: int = 128):
    """Abbreviate bytes, fill exceeding characters with ` ...`, for example
    `b'abcde ...'`"""
    if len(data) > length:
        prefix = repr(data[:length])[:-1]
        return "%s ... (%d bytes in total)" % (prefix, len(data))
    return repr(data)


class IndexedJsonlineReader(BaseIndexedReader[T]):
    """Random reading for jsonline files

    If fd_idx is None, attempt to build an index from fp_jsonline to
    support random reading
    """

    def _read_jsonline(self, file_object: BinaryIO, lineno: Optional[int] = None):
        line = file_object.readline()
        if not line:
            raise EOFError
        try:
            return json.loads(line)
        except json.JSONDecodeError as error:
            error_message = "failed to decode json: %r" % self.name
            if lineno is not None:
                error_message += ", lineno: %d" % lineno
            error_message += ", line: %r, because of %s" % (
                short_bytes(line),
                str(error),
            )
            raise InvalidJsonError(error_message)

    @classmethod
    def _build_index(
        cls,
        file_object: BinaryIO,
        offsets: Union[IndexHandlerReader, array],
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ) -> int:
        """Build index from jsonline file object

        :returns: the latest offset
        """
        shadow_jsonline = shadow_copy(file_object)
        current_offset = shadow_jsonline.tell()
        for line in shadow_jsonline:
            offsets.append(current_offset)
            current_offset = shadow_jsonline.tell()
            if index_build_callback:
                index_build_callback(line)
        return current_offset

    def _get(self, index: int) -> T:
        """Read a value by specified index

        :param index: Target value's index
        :returns: The read value
        """
        offset = self._offsets[index]
        self._file_object.seek(offset)
        try:
            value = self._read_jsonline(self._file_object, index)
        except EOFError:
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
        shadow_jsonline = shadow_copy(self._file_object)
        shadow_jsonline.seek(start_offset)
        index = start_index
        try:
            for index in range(start_index, end_index):
                yield self._read_jsonline(shadow_jsonline, index)
        except EOFError:
            end_offset = -1
            if end_index < self._count:
                end_offset = self._offsets[end_index]
            raise ValueError(
                "out of data: %r, index: %d, offset: %d ~ %d"
                % (self.name, index, start_offset, end_offset)
            )


class IndexedJsonlineWriter(BaseIndexedWriter[T]):
    """Used to write jsonline streams with index support"""

    def _append(self, value: T):
        self._file_object.write(json.dumps(value))
        self._file_object.write(NEWLINE)

    def _commit(self):
        self._file_object.flush()


def indexed_jsonline_open(
    path: str,
    mode: str = "r",
    *,
    index_path: Optional[str] = None,
    open_func: OpenBinaryIO = smart_open,
    index_build_callback: Optional[Callable[[Any], None]] = None,
) -> Union[IndexedJsonlineReader, IndexedJsonlineWriter]:
    """Open an indexed jsonline file

    .. note::
        When opening mode is ``r``, if ``.idx`` file doesn't exist, rebuild using
        jsonline file. (``.idx`` file corruption will not trigger rebuild, will
        report error)
        When opening mode is ``a``, if ``.idx`` file doesn't exist, consider
        ``.idx`` file as empty, equivalent to ``w`` mode.

    :param path: jsonline file path
    :param mode: Opening mode, supports read ``r``, write ``w``,
        and append ``a``, default ``r``
    :param index_path: Index file path, default is ``None``
    :param open_func: Open function for jsonline file stream
        Default uses smart_open
    :param index_build_callback: Callback function for building index
    :raises ValueError: Invalid mode
    :returns: Returns ``IndexedJsonlineReader`` when mode is ``r``,
        Returns ``IndexedJsonlineWriter`` when mode is ``w`` or ``a``
    """
    if mode not in ("r", "w", "a"):
        raise ValueError("unacceptable mode: %r" % mode)

    fp_mode = mode + "b"
    fp_jsonline_file = open_func(path, fp_mode)

    if index_path is None:
        index_path = path + INDEX_FILE_POSTFIX

    if mode == "r":
        return IndexedJsonlineReader(
            fp_jsonline_file,
            index_path,
            close_fileobj_when_close=True,
            index_build_callback=index_build_callback,
        )

    append_mode = mode in ("a",)

    return IndexedJsonlineWriter(
        fp_jsonline_file,
        index_path,
        append_mode=append_mode,
        close_fileobj_when_close=True,
    )

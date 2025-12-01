from array import array
from typing import Any, BinaryIO, Callable, Iterator, Optional, Union

from megfile import smart_open
from megfile.utils import shadow_copy

from megstore.indexed.base import (
    INDEX_FILE_POSTFIX,
    BaseIndexedReader,
    BaseIndexedWriter,
    IndexHandler,
)
from megstore.interface import OpenBinaryIO

__all__ = [
    "IndexedTxtReader",
    "IndexedTxtWriter",
    "indexed_txt_open",
]

NEWLINE = b"\n"


class IndexedTxtReader(BaseIndexedReader[str]):  # pytype: disable=not-indexable
    """Random reading for txt files

    If fd_idx is None, attempt to build an index from fp_jsonline to
    support random reading
    """

    def __init__(
        self,
        fp_data: BinaryIO,
        fp_index_path: Optional[str] = None,
        *,
        close_fileobj_when_close: bool = False,
        index_file_mode: str = "rb",
        index_build_callback: Optional[Callable[[Any], None]] = None,
        errors: str = "strict",
    ):
        super().__init__(
            fp_data,
            fp_index_path,
            close_fileobj_when_close=close_fileobj_when_close,
            index_file_mode=index_file_mode,
            index_build_callback=index_build_callback,
        )

        self._errors = errors

    @classmethod
    def _build_index(
        cls,
        file_object: BinaryIO,
        offsets: Union[IndexHandler, array],
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ) -> int:
        """Build index from txt stream

        :returns: the latest offset
        """
        shadow_file_object = shadow_copy(file_object)
        current_offset = shadow_file_object.tell()
        for line in shadow_file_object:
            offsets.append(current_offset)
            current_offset = shadow_file_object.tell()
            if index_build_callback:
                index_build_callback(line)
        return current_offset

    def _read_string(self, file_object: BinaryIO, lineno: Optional[int] = None):
        line = file_object.readline()
        if not line:
            raise EOFError
        return line.rstrip(NEWLINE).decode("utf-8", errors=self._errors)

    def _get(self, index: int) -> str:  # pytype: disable=invalid-annotation
        """Read a value by specified index

        :param index: Target value's index
        :returns: The read value
        """
        offset = self._offsets[index]
        self._file_object.seek(offset)
        try:
            value = self._read_string(self._file_object, index)
        except EOFError:
            raise ValueError(
                "out of data: %r, index: %d, offset: %d" % (self.name, index, offset)
            )
        return value

    def _batch_get(self, index_slice: slice) -> Iterator[str]:
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
        shadow_file_object = shadow_copy(self._file_object)
        shadow_file_object.seek(start_offset)
        index = start_index
        try:
            for index in range(start_index, end_index):
                yield self._read_string(shadow_file_object, index)
        except EOFError:
            end_offset = -1
            if end_index < self._count:
                end_offset = self._offsets[end_index]
            raise ValueError(
                "out of data: %r, index: %d, offset: %d ~ %d"
                % (self.name, index, start_offset, end_offset)
            )


class IndexedTxtWriter(BaseIndexedWriter[str]):  # pytype: disable=not-indexable
    """Used to write txt streams with index support"""

    def _append(self, value: str):
        self._file_object.write(value.encode("utf-8"))
        self._file_object.write(NEWLINE)

    def _commit(self):
        self._file_object.flush()


def indexed_txt_open(
    path: str,
    mode: str = "r",
    *,
    index_path: Optional[str] = None,
    open_func: OpenBinaryIO = smart_open,
    index_build_callback: Optional[Callable[[Any], None]] = None,
    errors: str = "strict",
) -> Union[IndexedTxtReader, IndexedTxtWriter]:
    """Open an indexed txt file

    .. note::
        When opening mode is ``r``, if ``.idx`` file doesn't exist,
        rebuild using txt file. (``.idx`` file corruption will not trigger rebuild,
        will report error)
        When opening mode is ``a``, if ``.idx`` file doesn't exist,
        consider ``.idx`` file as empty, equivalent to ``w`` mode.

    :param path: txt file path
    :param mode: Opening mode, supports read (``r``), write (``w``),
        and append (``a``), default ``r``
    :param index_path: Index file path, default is ``None``
    :param open_func: Open function for txt file stream
        Default uses ``smart_open``
    :param index_build_callback: Callback function for building index
    :param errors: errors parameter for decode
    :raises ValueError: Invalid mode
    :returns: Returns ``megstore.IndexedTxtReader`` when mode is ``r``,
        Returns ``megstore.IndexedTxtWriter`` when mode is ``w`` or ``a``
    """
    if mode not in ("r", "w", "a"):
        raise ValueError("unacceptable mode: %r" % mode)

    fp_mode = mode + "b"
    fp_jsonline_file = open_func(path, fp_mode)

    if index_path is None:
        index_path = path + INDEX_FILE_POSTFIX

    if mode == "r":
        return IndexedTxtReader(
            fp_jsonline_file,
            index_path,
            close_fileobj_when_close=True,
            index_build_callback=index_build_callback,
            errors=errors,
        )

    append_mode = mode in ("a",)

    return IndexedTxtWriter(
        fp_jsonline_file,
        index_path,
        append_mode=append_mode,
        close_fileobj_when_close=True,
    )

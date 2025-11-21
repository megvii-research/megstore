from abc import ABC, abstractmethod
from array import array
from struct import Struct
from typing import (
    Any,
    BinaryIO,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Union,
)

from megfile import smart_exists, smart_open
from megfile.errors import S3PermissionError
from megfile.utils import get_content_size, is_seekable, shadow_copy

from megstore.interface import (
    VT,
    Appendable,
    BaseReader,
    BaseWriter,
    Countable,
    Handler,
    OpenBinaryIO,
    SliceAccessible,
    T,
    smart_limited_seekable_open,
    validate_index,
)

INDEX_FILE_FORMAT = "Q"
INDEX_FILE_POSTFIX = ".idx"
INDEX_FILE_HEADER_FORMAT = "4c4cQ"
INDEX_FILE_HEADER_PREFIX = "IDV1"

# Index file validates correctness through file header:
# 1. File header struct format is '4c4cQ', first 4 c are prefix 'IDV1',
#   last 4 c record index file struct format,
#   default is 'Q   ' (padded to 4 characters), final Q records file size
# 2. When writing new files, or when index file doesn't exist or is
#    invalid, create new index file
# 3. When index file header content is incorrect (prefix not 'IDV1'),
#    or index file struct format doesn't match, or file size doesn't match,
#    rebuild index file
# 4. Index file records file pointer position for each data entry,
#    after index file validation passes, can quickly read corresponding
#    data through recorded file pointer positions


class BaseIndexedReader(BaseReader[T], ABC):
    """Random reading for files

    If fp_index is None, attempt to build an index from fp_data to
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
    ) -> None:
        """
        :param fp_data: File object
        :param fp_index_path: Corresponding index file path, default is None
        :param close_fileobj_when_close: When ``True``, close stream on exit,
            default is ``False``
        :param index_file_mode: Index file mode, default is ''rb''
        :param index_build_callback: Optional callback during index building,
            receives each item as parameter
        """
        super().__init__(fp_data, close_fileobj_when_close=close_fileobj_when_close)

        try:
            if not fp_index_path:
                raise OSError
            # Check if index file exists, create if it doesn't
            if (
                IndexHandlerReader.check_index_file_header(
                    fp_index_path, INDEX_FILE_FORMAT, fp_data
                )
                is False
            ):
                try:
                    with smart_limited_seekable_open(
                        fp_index_path, mode="wb"
                    ) as fp_index:
                        self.build_index(
                            fp_data=fp_data,
                            fp_index=fp_index,
                            index_build_callback=index_build_callback,
                        )
                finally:
                    fp_data.seek(0)

            fp_index = smart_limited_seekable_open(fp_index_path, mode=index_file_mode)
            self._offsets = IndexHandlerReader(
                fp_index,
                typecode=INDEX_FILE_FORMAT,
                header=INDEX_FILE_HEADER_FORMAT,
            )
            self._last_offset = None
        except (OSError, S3PermissionError):
            fp_index = None
            self._offsets = array(INDEX_FILE_FORMAT)
            self._last_offset = self._build_index(
                self._file_object,
                self._offsets,
                index_build_callback=index_build_callback,
            )

        self._count = len(self._offsets)

    @classmethod
    @abstractmethod
    def _build_index(
        cls,
        file_object: BinaryIO,
        offsets: Union["IndexHandlerReader", array],
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ) -> int:
        """Build index from data stream

        :returns: the latest offset
        """
        pass

    @classmethod
    def build_index(
        cls,
        fp_data: BinaryIO,
        fp_index: BinaryIO,
        index_build_callback: Optional[Callable[[Any], None]] = None,
    ):
        """Build index and write to index file

        :param fp_data: Data stream to build index from
        :param fp_index: Index file stream to write to
        :param index_build_callback: Optional callback during index building
        """
        offsets = IndexHandlerReader(
            fp_index, typecode=INDEX_FILE_FORMAT, header=INDEX_FILE_HEADER_FORMAT
        )
        offsets.write_header(get_content_size(fp_data), intrusive=False)
        cls._build_index(fp_data, offsets, index_build_callback=index_build_callback)

    @abstractmethod
    def _get(self, index: int) -> T:
        pass

    def get(self, index: int) -> T:
        """Get item at specified index

        :param index: Item index
        :returns: The item at the index
        """
        return self._get(validate_index(self, index))

    def _batch_get(self, index_slice: slice) -> Iterator[T]:
        """Get items for a slice of indices

        :param index_slice: Slice object specifying range
        :returns: Iterator of items
        """
        start, stop, step = index_slice.indices(self.count())
        for index in range(start, stop, step):
            yield self._get(index)

    def count(self):
        """Get total number of values in file.

        :returns: Total number of values
        """
        return self._count

    def _close(self):
        super()._close()
        if isinstance(self._offsets, IndexHandlerReader):
            self._offsets.close()


class BaseIndexedWriter(BaseWriter[T], ABC):
    """Used to write indexed jsonline streams"""

    def __init__(
        self,
        fp_data: BinaryIO,
        fp_index_path: str,
        *,
        append_mode: bool = False,
        close_fileobj_when_close: bool = False,
    ):
        """
        :param fp_data: File object
        :param fp_index_path: Corresponding index file path
        :param close_fileobj_when_close: When **True**, close stream on exit;
            when **False**, will not close stream
        """
        super().__init__(
            fp_data,
            append_mode=append_mode,
            close_fileobj_when_close=close_fileobj_when_close,
        )
        mode = "wb"
        if append_mode is True:
            mode = "ab"
        fp_index = smart_limited_seekable_open(fp_index_path, mode=mode)
        self._fp_index_path = fp_index_path
        self._offsets = IndexHandlerWriter(
            fp_index,
            append_mode=append_mode,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
        )
        if fp_index.tell() == 0:
            self._offsets.write_header(intrusive=True)

    def append(self, value: T):
        """Add a record

        :param value: Record to be added
        """
        offset = self._file_object.tell()
        self._append(value)
        self._offsets.append(offset)

    @abstractmethod
    def _append(self, value: T):
        pass

    def commit(self):
        """Write already added values to jsonline stream"""
        self._commit()
        self._offsets.commit()

    @abstractmethod
    def _commit(self):
        pass

    def _close(self):
        """Ensure data is written to stream and attempt to close stream"""
        size = get_content_size(self._file_object)
        super()._close()
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


class BaseIndexHandler(Appendable[VT], SliceAccessible[VT], Countable, Handler):
    DEFAULT_PAGE_SIZE = 16 * 2**10  # 16K
    def __init__(
        self, file_object: BinaryIO, *, close_fileobj_when_close: bool = False
    ):
        self._file_object = file_object
        self._close_fileobj_when_close = close_fileobj_when_close

    @property
    def name(self) -> str:
        if hasattr(self._file_object, "name"):
            return self._file_object.name
        return self._file_object

    def _close(self):
        if self._close_fileobj_when_close:
            self._file_object.close()

    def init_handler(
        self,
        typecode: str = INDEX_FILE_FORMAT,
        page_size: Optional[int] = None,
        header: Optional[str] = None,
    ):
        self._page_size = page_size or self.DEFAULT_PAGE_SIZE

        self._typecode = typecode
        self._struct = Struct(typecode)
        self._header = Struct(header) if header else None
        if is_seekable(self._file_object):
            self._content_size = get_content_size(self._file_object)
            if self._header and self._content_size:
                self._content_size = self._content_size - self._header.size
        else:
            self._content_size = 0
        if self._content_size % self._struct.size != 0 or self._content_size < 0:
            raise ValueError("unexpected trailing data: %r" % self.name)

    def write_header(self, size: int = 0, intrusive: bool = False):
        """Write index file header"""
        prefix_list = [s.encode() for s in INDEX_FILE_HEADER_PREFIX]
        format_list = [s.encode() for s in self._typecode.ljust(4)]
        header_data = self._header.pack(*prefix_list, *format_list, size)
        offset = self._file_object.tell()
        self._file_object.seek(0)
        self._file_object.write(header_data)
        if not intrusive:
            self._file_object.seek(offset)

    @classmethod
    def check_index_file_header(
        cls, index_path: str, typecode: str, file_object: BinaryIO
    ):
        """Check if index file header is correct"""
        if not smart_exists(index_path):
            return False
        try:
            header_struct = Struct(INDEX_FILE_HEADER_FORMAT)

            with smart_open(index_path, "rb") as fp_index:
                header_data = fp_index.read(header_struct.size)
                header = header_struct.unpack(header_data)

            magic = b"".join(header[:8]).decode(errors="backslashreplace")
            assert magic[:4] == INDEX_FILE_HEADER_PREFIX
            assert magic[4:8].strip() == typecode
            assert header[8] == get_content_size(file_object)

            return True
        except Exception:
            return False

    @property
    def mode(self) -> str:
        return self._file_object.mode

    def scan(self) -> Iterator[VT]:
        if self._header and self._file_object.tell() != self._header.size:
            self._file_object.seek(self._header.size)
        elif self._file_object.tell() != 0:
            self._file_object.seek(0)
        while True:
            page = self._file_object.read(self._page_size)
            if not page:
                break
            for item in self._struct.iter_unpack(page):
                yield item[0]

    def get(self, index: int) -> VT:
        index = validate_index(self, index)
        offset = index * self._struct.size
        if self._header:
            offset += self._header.size
        if self._file_object.tell() != offset:
            self._file_object.seek(offset)
        data = self._file_object.read(self._struct.size)
        item = self._struct.unpack(data)
        return item[0]

    def _batch_get(self, index_slice: slice) -> Iterator[VT]:
        start, stop, step = index_slice.indices(self.count())

        if step == 1:
            offset = start * self._struct.size
            if self._header:
                offset += self._header.size
            remain = (stop - start) * self._struct.size
            file_object = shadow_copy(self._file_object)
            if file_object.tell() != offset:
                file_object.seek(offset)
            while remain > 0:
                size = min(remain, self._page_size)
                page = file_object.read(size)
                remain -= len(page)
                if not page:
                    break
                for item in self._struct.iter_unpack(page):
                    yield item[0]
            return

        for index in range(start, stop, step):
            yield self.get(index)

    def put(self, index: int, value: VT):
        """Put value at specified index

        :param index: Index to put value at
        :param value: Value to put
        :raises IndexError: If index is out of range
        """
        # When index is less than count, behavior means overwrite
        # When index is greater than or equal to count, should report error
        index = validate_index(self, index)
        offset = index * self._struct.size
        if self._header:
            offset += self._header.size
        if self._file_object.tell() != offset:
            self._file_object.seek(offset)
        data = self._struct.pack(value)
        self._file_object.write(data)

    def batch_put(self, keys: Iterable[int], values: Iterable[VT]):
        """Put multiple values at specified indices

        :param keys: Iterable of indices
        :param values: Iterable of values to put
        :returns: List of results
        """
        return [self.put(key, value) for key, value in zip(keys, values)]

    def __setitem__(self, key: int, value: VT):
        """Set value at index (supports [] operator)

        :param key: Index to set
        :param value: Value to set
        """
        self.put(key, value)

    def count(self) -> int:
        return self._content_size // self._struct.size

    def append(self, value: VT):
        """Append value to the end

        :param value: Value to append
        """
        file_size = self._content_size
        if self._header:
            file_size += self._header.size
        if self._file_object.tell() != file_size:
            self._file_object.seek(file_size)
        data = self._struct.pack(value)
        self._file_object.write(data)
        self._content_size += self._struct.size


class IndexHandler(BaseIndexHandler[VT]):
    def __init__(
        self,
        file_object: BinaryIO,
        *,
        typecode: str = INDEX_FILE_FORMAT,
        page_size: Optional[int] = None,
        header: Optional[str] = None,
        close_fileobj_when_close: bool = False,
    ):
        """Initialize IndexHandler

        :param file_object: Binary file object
        :param typecode: Struct format code for index values
        :param page_size: Page size for reading, default 16K
        :param header: Optional header format
        :param close_fileobj_when_close: Whether to close file object on cleanup
        """
        super().__init__(
            file_object=file_object, close_fileobj_when_close=close_fileobj_when_close
        )
        self.init_handler(
            typecode=typecode,
            page_size=page_size,
            header=header,
        )


class IndexHandlerReader(BaseReader[VT], BaseIndexHandler[VT]):
    def __init__(
        self,
        file_object: BinaryIO,
        *,
        typecode: str = INDEX_FILE_FORMAT,
        page_size: Optional[int] = None,
        header: Optional[str] = None,
        close_fileobj_when_close: bool = False,
    ):
        """Initialize IndexHandlerReader

        :param file_object: Binary file object
        :param typecode: Struct format code for index values
        :param page_size: Page size for reading, default 16K
        :param header: Optional header format
        :param close_fileobj_when_close: Whether to close file object on cleanup
        """
        super().__init__(
            file_object=file_object, close_fileobj_when_close=close_fileobj_when_close
        )
        self.init_handler(
            typecode=typecode,
            page_size=page_size,
            header=header,
        )


class IndexHandlerWriter(BaseWriter[VT], BaseIndexHandler[VT]):
    def __init__(
        self,
        file_object: BinaryIO,
        *,
        typecode: str = INDEX_FILE_FORMAT,
        page_size: Optional[int] = None,
        header: Optional[str] = None,
        append_mode: bool = False,
        close_fileobj_when_close: bool = False,
    ):
        """Initialize IndexHandlerWriter

        :param file_object: Binary file object
        :param typecode: Struct format code for index values
        :param page_size: Page size for writing, default 16K
        :param header: Optional header format
        :param append_mode: Whether to open in append mode
        :param close_fileobj_when_close: Whether to close file object on cleanup
        """
        super().__init__(
            file_object=file_object,
            append_mode=append_mode,
            close_fileobj_when_close=close_fileobj_when_close,
        )
        self.init_handler(
            typecode=typecode,
            page_size=page_size,
            header=header,
        )

    def commit(self):
        self._file_object.flush()


def index_open(
    path: str, mode: str = "r", *, typecode: str, open_func: OpenBinaryIO = smart_open
):
    """Open an index file

    :param path: Path to index file
    :param mode: Open mode ('r', 'w', 'a', 'w+', 'a+')
    :param typecode: Struct format code for index values
    :param open_func: Function to open file
    :returns: IndexHandler instance
    :raises ValueError: If mode is invalid
    """
    if mode not in ("r", "w", "a", "w+", "a+"):
        raise ValueError("unacceptable mode: %r" % mode)

    fp_mode = mode + "b"
    if mode == "a+" and smart_exists(path):
        fp_mode = "rb+"
    elif mode in ("w+", "a+"):
        fp_mode = "wb+"

    fp_file = open_func(path, fp_mode)
    return IndexHandler(fp_file, typecode=typecode)

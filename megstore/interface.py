from abc import ABC, abstractmethod
from functools import partial
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    TextIO,
    Tuple,
    TypeVar,
    Union,
)

from megfile import smart_open
from megfile.interfaces import Closable
from megfile.utils import ThreadLocal, shadow_copy

T = TypeVar("T")
KT = TypeVar("KT")  # key type
VT = TypeVar("VT")  # value type


def reopen(file_object) -> Tuple[IO, bool]:
    if not hasattr(file_object, "name") or not hasattr(file_object, "mode"):
        return shadow_copy(file_object), False
    file_object = smart_open(file_object.name, file_object.mode)
    file_object.seek(file_object.tell())
    return file_object, True


class Handler(Closable, ABC):
    @property
    @abstractmethod
    def name(self) -> Any:
        pass

    @property
    @abstractmethod
    def mode(self) -> str:
        pass

    def __repr__(self):
        name = self.__class__.__qualname__
        return "%s(%r, %r)" % (name, self.name, self.mode)


class Countable(ABC):
    @abstractmethod
    def count(self) -> int:
        return 0

    def __len__(self) -> int:
        return self.count()


def make_slice(index: range) -> slice:
    stop = None if index.stop == -1 else index.stop
    return slice(index.start, stop, index.step)


class IterableValue(Iterable[VT]):
    def __init__(self, index: range, get_func: Callable, batch_get_func: Callable):
        self._index = index
        self._get_func = get_func
        self._batch_get_func = batch_get_func

    def __repr__(self):
        name = self.__class__.__qualname__
        return "%s(%s)" % (name, list(self))

    def __len__(self) -> int:
        return len(self._index)

    def __getitem__(self, index: Union[int, slice]) -> "Union[VT, IterableValue[VT]]":
        if isinstance(index, slice):
            return self.__class__(
                index=self._index[index],
                get_func=self._get_func,
                batch_get_func=self._batch_get_func,
            )
        return self._get_func(self._index[index])

    def __iter__(self):
        yield from self._batch_get_func(make_slice(self._index))


class SliceAccessible(Generic[VT], Countable, ABC):
    @abstractmethod
    def get(self, key: int) -> VT:
        pass

    def _batch_get(self, index_slice: slice) -> Iterator[VT]:
        for index in range(self.count())[index_slice]:
            yield self.get(index)

    def batch_get(self, index: slice) -> IterableValue[VT]:
        return IterableValue(
            index=range(self.count())[index],
            get_func=self.get,
            batch_get_func=self._batch_get,
        )

    def __getitem__(self, index: Union[int, slice]) -> Union[VT, List[VT]]:
        if isinstance(index, slice):
            return self.batch_get(index)
        return self.get(index)

    def __iter__(self) -> Iterator[VT]:
        yield from self._batch_get(slice(0, self.count(), 1))


class Appendable(Generic[VT], ABC):
    @abstractmethod
    def append(self, value: VT):
        pass

    def extend(self, values: Iterable[VT]):
        for value in values:
            self.append(value)


OpenIO = Callable[[str, str], IO]
OpenTextIO = Callable[[str, str], TextIO]
OpenBinaryIO = Callable[[str, str], BinaryIO]


class BaseReader(SliceAccessible[T], Handler, ABC):
    def __init__(self, file_object: BinaryIO, *, close_fileobj_when_close: bool = True):
        self._raw_file_object = file_object
        self._close_fileobj_when_close = close_fileobj_when_close

        self._local = ThreadLocal()
        self._local["file_object"] = file_object

    @property
    def _clean_reopen(self):
        if not hasattr(self, "_local"):
            return False
        return self._local.get("_clean_reopen", False)

    @_clean_reopen.setter
    def _clean_reopen(self, value: bool):
        self._local["_clean_reopen"] = value

    def _create_file_object(self):
        new_file_object, is_reopen = reopen(self._raw_file_object)
        if is_reopen:
            self._clean_reopen = True
        return new_file_object

    @property
    def _file_object(self):
        return self._local("file_object", self._create_file_object)

    @property
    def mode(self) -> str:
        return "r"

    @property
    def name(self) -> Any:
        if hasattr(self._file_object, "name"):
            return self._file_object.name
        return self._file_object

    def _close_reopen(self):
        if self._clean_reopen:
            self._file_object.close()
            self._clean_reopen = False

    def _close(self):
        if self._close_fileobj_when_close:
            self._raw_file_object.close()
        self._close_reopen()

    def __del__(self):
        self._close_reopen()


class BaseWriter(Appendable[T], Handler):
    def __init__(
        self,
        file_object: BinaryIO,
        *,
        append_mode: bool = False,
        close_fileobj_when_close: bool = True,
    ):
        self._file_object = file_object
        self._close_fileobj_when_close = close_fileobj_when_close
        self._append_mode = append_mode

    @property
    def name(self) -> Any:
        if hasattr(self._file_object, "name"):
            return self._file_object.name
        return self._file_object

    @property
    def mode(self) -> str:
        if self._append_mode:
            return "a"
        return "w"

    @abstractmethod
    def commit(self):
        pass

    def _close(self):
        self.commit()
        if self._close_fileobj_when_close:
            self._file_object.close()


smart_limited_seekable_open = partial(smart_open, limited_seekable=True)

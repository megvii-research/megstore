from io import BytesIO
from unittest.mock import MagicMock, Mock

from megstore.interface import (
    Appendable,
    BaseReader,
    BaseWriter,
    Countable,
    Handler,
    IterableValue,
    SliceAccessible,
    make_slice,
    reopen,
)


class TestReopen:
    """Tests for the reopen function"""

    def test_reopen_without_name_attribute(self):
        """Test reopen with file object without name attribute"""
        file_obj = BytesIO(b"test data")
        new_obj, is_reopen = reopen(file_obj)
        # Should return a shadow copy
        assert is_reopen is False
        assert new_obj is not file_obj

    def test_reopen_without_mode_attribute(self):
        """Test reopen with file object without mode attribute"""
        file_obj = Mock()
        file_obj.name = "test.txt"
        del file_obj.mode  # Ensure no mode attribute
        new_obj, is_reopen = reopen(file_obj)
        assert is_reopen is False


class TestHandler:
    """Tests for Handler abstract class"""

    def test_handler_repr(self):
        """Test Handler __repr__ method"""

        class ConcreteHandler(Handler):
            def __init__(self, name, mode):
                self._name = name
                self._mode = mode

            @property
            def name(self):
                return self._name

            @property
            def mode(self):
                return self._mode

            def _close(self):
                pass

        handler = ConcreteHandler("test.txt", "r")
        repr_str = repr(handler)
        assert "ConcreteHandler" in repr_str
        assert "test.txt" in repr_str
        assert "r" in repr_str


class TestCountable:
    """Tests for Countable abstract class"""

    def test_countable_len(self):
        """Test Countable __len__ method"""

        class ConcreteCountable(Countable):
            def count(self):
                return 42

        countable = ConcreteCountable()
        assert len(countable) == 42
        assert countable.count() == 42


class TestMakeSlice:
    """Tests for make_slice function"""

    def test_make_slice_with_stop_minus_one(self):
        """Test make_slice with stop=-1"""
        index = range(0, -1, 1)
        result = make_slice(index)
        assert result.start == 0
        assert result.stop is None
        assert result.step == 1

    def test_make_slice_normal(self):
        """Test make_slice with normal range"""
        index = range(1, 5, 2)
        result = make_slice(index)
        assert result.start == 1
        assert result.stop == 5
        assert result.step == 2


class TestIterableValue:
    """Tests for IterableValue class"""

    def test_iterable_value_repr(self):
        """Test IterableValue __repr__ method"""
        get_func = lambda i: i * 2  # noqa: E731
        batch_get_func = lambda s: iter(range(s.start, s.stop))  # noqa: E731
        iterable = IterableValue(range(0, 3), get_func, batch_get_func)
        repr_str = repr(iterable)
        assert "IterableValue" in repr_str

    def test_iterable_value_len(self):
        """Test IterableValue __len__ method"""
        get_func = lambda i: i  # noqa: E731
        batch_get_func = lambda s: iter([])  # noqa: E731
        iterable = IterableValue(range(0, 5), get_func, batch_get_func)
        assert len(iterable) == 5

    def test_iterable_value_getitem_int(self):
        """Test IterableValue __getitem__ with int"""
        get_func = lambda i: i * 2  # noqa: E731
        batch_get_func = lambda s: iter([])  # noqa: E731
        iterable = IterableValue(range(0, 5), get_func, batch_get_func)
        assert iterable[0] == 0
        assert iterable[1] == 2
        assert iterable[2] == 4

    def test_iterable_value_getitem_slice(self):
        """Test IterableValue __getitem__ with slice"""
        get_func = lambda i: i * 2  # noqa: E731
        batch_get_func = lambda s: iter([])  # noqa: E731
        iterable = IterableValue(range(0, 5), get_func, batch_get_func)
        sliced = iterable[1:3]
        assert isinstance(sliced, IterableValue)
        assert len(sliced) == 2

    def test_iterable_value_iter(self):
        """Test IterableValue __iter__ method"""
        get_func = lambda i: i * 2  # noqa: E731

        def batch_get_func(s):
            for i in range(s.start, s.stop, s.step):
                yield i * 2

        iterable = IterableValue(range(0, 3), get_func, batch_get_func)
        result = list(iterable)
        assert result == [0, 2, 4]


class TestSliceAccessible:
    """Tests for SliceAccessible abstract class"""

    def test_slice_accessible_batch_get(self):
        """Test SliceAccessible batch_get method"""

        class ConcreteSliceAccessible(SliceAccessible):
            def __init__(self, data):
                self._data = data

            def get(self, index):
                return self._data[index]

            def count(self):
                return len(self._data)

        accessible = ConcreteSliceAccessible([1, 2, 3, 4, 5])
        result = accessible.batch_get(slice(1, 4))
        assert isinstance(result, IterableValue)
        assert list(result) == [2, 3, 4]

    def test_slice_accessible_getitem_int(self):
        """Test SliceAccessible __getitem__ with int"""

        class ConcreteSliceAccessible(SliceAccessible):
            def __init__(self, data):
                self._data = data

            def get(self, index):
                return self._data[index]

            def count(self):
                return len(self._data)

        accessible = ConcreteSliceAccessible([1, 2, 3])
        assert accessible[0] == 1
        assert accessible[1] == 2
        assert accessible[2] == 3

    def test_slice_accessible_getitem_slice(self):
        """Test SliceAccessible __getitem__ with slice"""

        class ConcreteSliceAccessible(SliceAccessible):
            def __init__(self, data):
                self._data = data

            def get(self, index):
                return self._data[index]

            def count(self):
                return len(self._data)

        accessible = ConcreteSliceAccessible([1, 2, 3, 4, 5])
        result = accessible[1:4]
        assert list(result) == [2, 3, 4]

    def test_slice_accessible_iter(self):
        """Test SliceAccessible __iter__ method"""

        class ConcreteSliceAccessible(SliceAccessible):
            def __init__(self, data):
                self._data = data

            def get(self, index):
                return self._data[index]

            def count(self):
                return len(self._data)

        accessible = ConcreteSliceAccessible([1, 2, 3])
        result = list(accessible)
        assert result == [1, 2, 3]


class TestAppendable:
    """Tests for Appendable abstract class"""

    def test_appendable_extend(self):
        """Test Appendable extend method"""

        class ConcreteAppendable(Appendable):
            def __init__(self):
                self._data = []

            def append(self, value):
                self._data.append(value)

        appendable = ConcreteAppendable()
        appendable.extend([1, 2, 3])
        assert appendable._data == [1, 2, 3]


class TestBaseReader:
    """Tests for BaseReader class"""

    def test_base_reader_mode(self):
        """Test BaseReader mode property"""

        class ConcreteReader(BaseReader):
            def get(self, index):
                return index

            def count(self):
                return 0

        file_obj = BytesIO(b"test")
        reader = ConcreteReader(file_obj)
        assert reader.mode == "r"
        reader._close()

    def test_base_reader_name_with_name_attr(self):
        """Test BaseReader name property when file has name"""

        class ConcreteReader(BaseReader):
            def get(self, index):
                return index

            def count(self):
                return 0

        file_obj = MagicMock()
        file_obj.name = "test.txt"
        reader = ConcreteReader(file_obj)
        assert reader.name == "test.txt"

    def test_base_reader_name_without_name_attr(self):
        """Test BaseReader name property when file has no name"""

        class ConcreteReader(BaseReader):
            def get(self, index):
                return index

            def count(self):
                return 0

        file_obj = BytesIO(b"test")
        reader = ConcreteReader(file_obj)
        assert reader.name == file_obj


class TestBaseWriter:
    """Tests for BaseWriter class"""

    def test_base_writer_mode_write(self):
        """Test BaseWriter mode property in write mode"""

        class ConcreteWriter(BaseWriter):
            def append(self, value):
                pass

            def commit(self):
                pass

            def _close(self):
                pass

        file_obj = BytesIO()
        writer = ConcreteWriter(file_obj, append_mode=False)
        assert writer.mode == "w"

    def test_base_writer_mode_append(self):
        """Test BaseWriter mode property in append mode"""

        class ConcreteWriter(BaseWriter):
            def append(self, value):
                pass

            def commit(self):
                pass

            def _close(self):
                pass

        file_obj = BytesIO()
        writer = ConcreteWriter(file_obj, append_mode=True)
        assert writer.mode == "a"

    def test_base_writer_name_with_name_attr(self):
        """Test BaseWriter name property when file has name"""

        class ConcreteWriter(BaseWriter):
            def append(self, value):
                pass

            def commit(self):
                pass

            def _close(self):
                pass

        file_obj = MagicMock()
        file_obj.name = "output.txt"
        writer = ConcreteWriter(file_obj)
        assert writer.name == "output.txt"

    def test_base_writer_name_without_name_attr(self):
        """Test BaseWriter name property when file has no name"""

        class ConcreteWriter(BaseWriter):
            def append(self, value):
                pass

            def commit(self):
                pass

            def _close(self):
                pass

        file_obj = BytesIO()
        writer = ConcreteWriter(file_obj)
        assert writer.name == file_obj

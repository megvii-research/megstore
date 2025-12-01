import struct
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from megstore.indexed.base import (
    INDEX_FILE_FORMAT,
    INDEX_FILE_HEADER_FORMAT,
    BaseIndexHandler,
    IndexHandler,
    IndexHandlerReader,
    IndexHandlerWriter,
    index_open,
    validate_index,
)

INDEX_FILE_RECROD_SIZE = struct.calcsize(INDEX_FILE_FORMAT)
INDEX_FILE_HEADER_SIZE = struct.calcsize(INDEX_FILE_HEADER_FORMAT)


def generate_index_header(size=0):
    return struct.Struct(INDEX_FILE_HEADER_FORMAT).pack(
        b"I", b"D", b"V", b"1", b"Q", b" ", b" ", b" ", size
    )


class TestIndexHandler:
    """Tests for IndexHandler class"""

    def test_index_handler_init(self):
        """Test IndexHandler initialization"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)
        assert handler.count() == 0
        handler.close()

    def test_index_handler_append_and_get(self):
        """Test IndexHandler append and get methods"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)
        handler.append(300)

        assert handler.count() == 3
        assert handler.get(0) == 100
        assert handler.get(1) == 200
        assert handler.get(2) == 300
        handler.close()

    def test_index_handler_getitem(self):
        """Test IndexHandler __getitem__ method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)

        assert handler[0] == 100
        assert handler[1] == 200
        assert handler[-1] == 200
        assert handler[-2] == 100
        handler.close()

    def test_index_handler_put(self):
        """Test IndexHandler put method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)

        handler.put(0, 150)
        assert handler.get(0) == 150
        assert handler.get(1) == 200
        handler.close()

    def test_index_handler_setitem(self):
        """Test IndexHandler __setitem__ method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)

        handler[0] = 150
        assert handler[0] == 150
        handler.close()

    def test_index_handler_batch_put(self):
        """Test IndexHandler batch_put method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)
        handler.append(300)

        handler.batch_put([0, 2], [150, 350])
        assert handler.get(0) == 150
        assert handler.get(1) == 200
        assert handler.get(2) == 350
        handler.close()

    def test_index_handler_scan(self):
        """Test IndexHandler scan method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)
        handler.append(300)

        result = list(handler.scan())
        assert result == [100, 200, 300]
        handler.close()

    def test_index_handler_batch_get_step_1(self):
        """Test IndexHandler _batch_get with step=1"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        for i in range(10):
            handler.append(i * 100)

        result = list(handler._batch_get(slice(2, 5)))
        assert result == [200, 300, 400]
        handler.close()

    def test_index_handler_batch_get_step_not_1(self):
        """Test IndexHandler _batch_get with step != 1"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        for i in range(10):
            handler.append(i * 100)

        result = list(handler._batch_get(slice(0, 10, 2)))
        assert result == [0, 200, 400, 600, 800]
        handler.close()

    def test_index_handler_iter(self):
        """Test IndexHandler __iter__ method"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)

        handler.append(100)
        handler.append(200)
        handler.append(300)

        result = list(handler)
        assert result == [100, 200, 300]
        handler.close()

    def test_index_handler_name(self):
        """Test IndexHandler name property"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)
        assert handler.name == "BytesIO"
        handler.close()

    def test_index_handler_mode(self):
        """Test IndexHandler mode property"""
        file_obj = BytesIO()
        file_obj.mode = "rb+"  # Mock mode attribute
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)
        # BytesIO doesn't have mode attribute by default
        # The mode property should return the file_object's mode
        assert handler.mode == getattr(file_obj, "mode", None)
        handler.close()

    def test_index_handler_with_header(self):
        """Test IndexHandler with header"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
        )

        handler.write_header(size=100)
        handler.append(100)
        handler.append(200)

        assert handler.count() == 2
        assert handler.get(0) == 100
        assert handler.get(1) == 200
        handler.close()

    def test_index_handler_write_header_intrusive(self):
        """Test IndexHandler write_header with intrusive=True"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
        )

        handler.write_header(size=100, intrusive=True)
        # After intrusive write, position should be at header end
        assert file_obj.tell() == INDEX_FILE_HEADER_SIZE
        handler.close()

    def test_index_handler_write_header_non_intrusive(self):
        """Test IndexHandler write_header with intrusive=False"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
        )

        handler.write_header(size=100, intrusive=False)
        assert file_obj.tell() == 0  # Position should remain unchanged
        handler.close()


class TestIndexHandlerReader:
    """Tests for IndexHandlerReader class"""

    def test_index_handler_reader(self):
        """Test IndexHandlerReader basic functionality"""
        # First write some data
        file_obj = BytesIO()
        writer = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)
        writer.append(100)
        writer.append(200)
        writer.append(300)
        writer.close()

        # Now read it back
        file_obj.seek(0)
        reader = IndexHandlerReader(file_obj, typecode=INDEX_FILE_FORMAT)
        assert reader.count() == 3
        assert reader.get(0) == 100
        assert reader.get(1) == 200
        assert reader.get(2) == 300
        reader.close()


class TestIndexHandlerWriter:
    """Tests for IndexHandlerWriter class"""

    def test_index_handler_writer(self):
        """Test IndexHandlerWriter basic functionality"""
        file_obj = BytesIO()
        writer = IndexHandlerWriter(file_obj, typecode=INDEX_FILE_FORMAT)
        writer.append(100)
        writer.append(200)
        writer.commit()
        assert writer.count() == 2
        writer.close()

    def test_index_handler_writer_append_mode(self):
        """Test IndexHandlerWriter in append mode"""
        file_obj = BytesIO()
        writer = IndexHandlerWriter(
            file_obj, typecode=INDEX_FILE_FORMAT, append_mode=True
        )
        writer.append(100)
        writer.commit()
        writer.close()

        writer = IndexHandlerWriter(
            file_obj, typecode=INDEX_FILE_FORMAT, append_mode=True
        )
        writer.append(200)
        writer.commit()
        assert writer.count() == 2
        writer.close()


class TestIndexOpen:
    """Tests for index_open function"""

    def test_index_open_read_mode(self, fs):
        """Test index_open in read mode"""
        # Create a file first
        with open("test.idx", "wb") as f:
            f.write(struct.pack(INDEX_FILE_FORMAT, 100))
            f.write(struct.pack(INDEX_FILE_FORMAT, 200))

        handler = index_open("test.idx", mode="r", typecode=INDEX_FILE_FORMAT)
        assert handler.count() == 2
        handler.close()

    def test_index_open_write_mode(self, fs):
        """Test index_open in write mode"""
        handler = index_open("test.idx", mode="w", typecode=INDEX_FILE_FORMAT)
        handler.append(100)
        # Flush the data by seeking
        handler._file_object.flush()
        handler.close()

        # Verify the handler was created correctly
        assert isinstance(handler, IndexHandler)

    def test_index_open_append_mode(self, fs):
        """Test index_open in append mode"""
        # First write to create file
        handler1 = index_open("test.idx", mode="w", typecode=INDEX_FILE_FORMAT)
        handler1.append(100)
        handler1._file_object.flush()
        handler1.close()

        # Then append
        handler2 = index_open("test.idx", mode="a", typecode=INDEX_FILE_FORMAT)
        handler2.append(200)
        handler2._file_object.flush()
        handler2.close()

        # Verify the handler was created correctly
        assert handler2 is not None

    def test_index_open_write_plus_mode(self, fs):
        """Test index_open in w+ mode"""
        handler = index_open("test.idx", mode="w+", typecode=INDEX_FILE_FORMAT)
        handler.append(100)
        assert handler.get(0) == 100
        handler.close()

    def test_index_open_append_plus_mode(self, fs):
        """Test index_open in a+ mode"""
        # Create a file first
        with open("test.idx", "wb") as f:
            f.write(struct.pack(INDEX_FILE_FORMAT, 100))

        handler = index_open("test.idx", mode="a+", typecode=INDEX_FILE_FORMAT)
        assert handler.get(0) == 100
        handler.append(200)
        handler.close()

    def test_index_open_invalid_mode(self, fs):
        """Test index_open with invalid mode"""
        with pytest.raises(ValueError) as error:
            index_open("test.idx", mode="invalid", typecode=INDEX_FILE_FORMAT)
        assert "unacceptable mode" in str(error.value)


class TestBaseIndexHandler:
    """Tests for BaseIndexHandler class"""

    def test_check_index_file_header_not_exists(self, fs):
        """Test check_index_file_header when file doesn't exist"""
        result = BaseIndexHandler.check_index_file_header(
            "nonexistent.idx", INDEX_FILE_FORMAT, BytesIO()
        )
        assert result is False

    def test_check_index_file_header_invalid_magic(self, fs):
        """Test check_index_file_header with invalid magic"""
        with open("test.idx", "wb") as f:
            f.write(b"INVALID_HEADER_DATA")

        result = BaseIndexHandler.check_index_file_header(
            "test.idx", INDEX_FILE_FORMAT, BytesIO()
        )
        assert result is False

    def test_check_index_file_header_wrong_size(self, fs):
        """Test check_index_file_header with wrong size"""
        # Create a valid header but with wrong size
        file_obj = BytesIO(b"test data")

        with open("test.idx", "wb") as f:
            # Write header with wrong size
            header = generate_index_header(size=999)
            f.write(header)

        result = BaseIndexHandler.check_index_file_header(
            "test.idx", INDEX_FILE_FORMAT, file_obj
        )
        assert result is False

    def test_check_index_file_header_valid(self, fs):
        """Test check_index_file_header with valid header"""
        file_obj = BytesIO(b"test data")
        file_size = len(b"test data")

        with open("test.idx", "wb") as f:
            header = generate_index_header(size=file_size)
            f.write(header)

        result = BaseIndexHandler.check_index_file_header(
            "test.idx", INDEX_FILE_FORMAT, file_obj
        )
        assert result is True

    def test_index_handler_invalid_content_size(self):
        """Test IndexHandler with invalid content size"""
        # Create file with invalid size (not multiple of struct size)
        file_obj = BytesIO(b"abc")  # 3 bytes, not multiple of 8
        with pytest.raises(ValueError) as error:
            IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT)
        assert "unexpected trailing data" in str(error.value)

    def test_index_handler_close_fileobj_when_close(self):
        """Test IndexHandler close_fileobj_when_close option"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj, typecode=INDEX_FILE_FORMAT, close_fileobj_when_close=True
        )
        handler.close()
        assert file_obj.closed

    def test_index_handler_no_close_fileobj_when_close(self):
        """Test IndexHandler without close_fileobj_when_close"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj, typecode=INDEX_FILE_FORMAT, close_fileobj_when_close=False
        )
        handler.close()
        assert not file_obj.closed

    def test_index_handler_scan_with_header(self):
        """Test IndexHandler scan with header"""
        file_obj = BytesIO()
        handler = IndexHandler(
            file_obj,
            typecode=INDEX_FILE_FORMAT,
            header=INDEX_FILE_HEADER_FORMAT,
        )
        handler.write_header(size=0, intrusive=True)
        handler.append(100)
        handler.append(200)

        # Scan should work correctly with header
        result = list(handler.scan())
        assert result == [100, 200]
        handler.close()

    def test_index_handler_custom_page_size(self):
        """Test IndexHandler with custom page size"""
        file_obj = BytesIO()
        handler = IndexHandler(file_obj, typecode=INDEX_FILE_FORMAT, page_size=64)
        assert handler._page_size == 64
        handler.close()


class TestValidateIndex:
    """Tests for validate_index function"""

    def test_validate_index_positive(self):
        """Test validate_index with positive index"""
        handler = MagicMock()
        handler.__len__ = MagicMock(return_value=5)
        assert validate_index(handler, 0) == 0
        assert validate_index(handler, 2) == 2
        assert validate_index(handler, 4) == 4

    def test_validate_index_negative(self):
        """Test validate_index with negative index"""
        handler = MagicMock()
        handler.__len__ = MagicMock(return_value=5)
        assert validate_index(handler, -1) == 4
        assert validate_index(handler, -3) == 2
        assert validate_index(handler, -5) == 0

    def test_validate_index_out_of_range_positive(self):
        """Test validate_index with out of range positive index"""
        handler = MagicMock()
        handler.__len__ = MagicMock(return_value=5)
        handler.name = "test_handler"
        with pytest.raises(IndexError) as error:
            validate_index(handler, 5)
        assert "index out of range" in str(error.value)
        assert "test_handler" in str(error.value)

    def test_validate_index_out_of_range_negative(self):
        """Test validate_index with out of range negative index"""
        handler = MagicMock()
        handler.__len__ = MagicMock(return_value=5)
        handler.name = "test_handler"
        with pytest.raises(IndexError) as error:
            validate_index(handler, -6)
        assert "index out of range" in str(error.value)

    def test_validate_index_without_name(self):
        """Test validate_index with handler without name attribute"""
        handler = MagicMock()
        handler.__len__ = MagicMock(return_value=5)
        del handler.name  # Remove name attribute
        with pytest.raises(IndexError) as error:
            validate_index(handler, 10)
        assert "index out of range" in str(error.value)

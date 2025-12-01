import random
import struct
from io import BytesIO
from typing import List

import pytest
from megfile import smart_open
from megfile.utils import get_content_size

from megstore.indexed.base import (
    INDEX_FILE_FORMAT,
    INDEX_FILE_HEADER_FORMAT,
    INDEX_FILE_POSTFIX,
)
from megstore.indexed.txt import (
    IndexedTxtReader,
    IndexedTxtWriter,
    indexed_txt_open,
)

INDEX_FILE_RECROD_SIZE = struct.calcsize(INDEX_FILE_FORMAT)
INDEX_FILE_HEADER_SIZE = struct.calcsize(INDEX_FILE_HEADER_FORMAT)
EMPTY_INDEX_CONTENT = struct.Struct(INDEX_FILE_HEADER_FORMAT).pack(
    b"I", b"D", b"V", b"1", b"Q", b" ", b" ", b" ", 0
)


def generate_index_header(size=0):
    return struct.Struct(INDEX_FILE_HEADER_FORMAT).pack(
        b"I", b"D", b"V", b"1", b"Q", b" ", b" ", b" ", size
    )


values = [
    "line1",
    "line2",
    "line3 with spaces",
    "line4",
    "line5 longer line here",
    "line6",
]


def unpack_indexes(data: bytes) -> List[int]:
    """
    Unpack serialized indexes bytes
    """
    offsets = []
    data = data[INDEX_FILE_HEADER_SIZE:]
    for i in range(len(data) // INDEX_FILE_RECROD_SIZE):
        offset_bytes = data[
            i * INDEX_FILE_RECROD_SIZE : (i + 1) * INDEX_FILE_RECROD_SIZE
        ]
        offset = struct.unpack(INDEX_FILE_FORMAT, offset_bytes)[0]
        offsets.append(offset)
    return offsets


def test_indexed_txt_writer(fs):
    txt_stream = BytesIO()
    index_path = "key.idx"
    with IndexedTxtWriter(
        txt_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        assert writer._file_object.tell() == 0
        writer.append(values[0])  # "line1" -> 5 bytes + newline = 6 bytes
        assert writer._file_object.tell() == 6
        assert txt_stream.getvalue()[-1:] == b"\n"

        writer.extend(values[1:])

        writer.commit()

    assert txt_stream.closed
    with smart_open(index_path, "rb") as index_reader:
        assert (
            get_content_size(index_reader) == len(values) * 8 + INDEX_FILE_HEADER_SIZE
        )


def test_indexed_txt_writer_with_context_manager_without_close(fs):
    txt_stream = BytesIO()
    index_path = "key.idx"
    with IndexedTxtWriter(txt_stream, index_path, close_fileobj_when_close=False):
        pass
    assert not txt_stream.closed
    assert txt_stream.getvalue() == b""
    txt_stream.close()
    with smart_open(index_path, "rb") as index_reader:
        assert index_reader.read() == EMPTY_INDEX_CONTENT


def test_indexed_txt_writer_with_context_manager_auto_close(fs):
    txt_stream = open("auto_close.txt", "wb")
    index_path = "auto_close.txt%s" % INDEX_FILE_POSTFIX
    with IndexedTxtWriter(txt_stream, index_path, close_fileobj_when_close=True):
        pass
    assert txt_stream.closed

    with open("auto_close.txt", "rb") as txt_stream:
        assert txt_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_txt_writer_with_context_manager_do_close_in_context(fs):
    txt_stream = open("file.txt", "wb")
    index_path = "file.txt%s" % INDEX_FILE_POSTFIX
    with IndexedTxtWriter(
        txt_stream,
        index_path,
        close_fileobj_when_close=False,
    ) as writer:
        writer.close()
    assert not txt_stream.closed

    txt_stream.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_txt_writer_with_context_manager_do_close_after_context(fs):
    txt_stream = open("file.txt", "wb")
    index_path = "file.txt%s" % INDEX_FILE_POSTFIX
    with IndexedTxtWriter(
        txt_stream,
        index_path,
        close_fileobj_when_close=False,
    ) as writer:
        pass
    assert not txt_stream.closed

    writer.close()
    assert not txt_stream.closed

    txt_stream.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_txt_writer_with_context_manager_do_multiple_close(fs):
    txt_stream = open("file.txt", "wb")
    index_path = "file.txt%s" % INDEX_FILE_POSTFIX
    with IndexedTxtWriter(
        txt_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        writer.close()
        assert txt_stream.closed
    writer.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_txt_writer_without_context_manager(fs):
    txt_stream = open("file.txt", "wb")
    index_path = "file.txt%s" % INDEX_FILE_POSTFIX
    writer = IndexedTxtWriter(txt_stream, index_path)
    writer.commit()

    assert not txt_stream.closed

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT

    writer.append("test")

    # 多次 commit 和 close
    writer.commit()
    writer.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b"test\n"


def test_indexed_txt_writer_append_mode(fs):
    with indexed_txt_open("file.txt", "a") as writer:
        writer.append("line1")

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b"line1\n"

    with indexed_txt_open("file.txt", "a") as writer:
        writer.append("line2")
    txt_stream.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b"line1\nline2\n"

    with indexed_txt_open("file.txt", "w") as writer:
        writer.append("line3")
    txt_stream.close()

    with open("file.txt", "rb") as txt_stream:
        assert txt_stream.read() == b"line3\n"


@pytest.fixture
def txt_stream():
    txt_stream = BytesIO()
    for value in values:
        txt_stream.write(value.encode("utf-8"))
        txt_stream.write(b"\n")
    txt_stream.seek(0)
    yield txt_stream


def test_indexed_txt_reader_without_index(txt_stream):
    with IndexedTxtReader(txt_stream) as reader:
        assert len(reader) == 6

        # 随机读
        assert reader[0] == "line1"
        assert reader[4] == "line5 longer line here"
        assert reader[1] == "line2"
        assert reader[2] == "line3 with spaces"
        assert reader[5] == "line6"
        assert reader[3] == "line4"
        assert reader[-1] == "line6"
        assert reader[-4] == "line3 with spaces"

        for _ in range(100):
            index = random.randint(-6, 5)
            assert values[index] == reader[index]

        # over index
        with pytest.raises(IndexError) as error:
            reader[-7]
        assert repr(txt_stream) in str(error.value)

        with pytest.raises(IndexError) as error:
            reader[6]
        assert repr(txt_stream) in str(error.value)

        # scan, iter
        assert [x for x in reader] == values


def test_indexed_txt_reader_read_by_slice(txt_stream):
    with IndexedTxtReader(txt_stream) as reader:
        # 切片
        assert list(reader[:]) == values[:]
        assert list(reader[::]) == values[::]
        assert list(reader[1:]) == values[1:]
        assert list(reader[:-1]) == values[:-1]
        assert list(reader[::-1]) == values[::-1]
        assert list(reader[::-2]) == values[::-2]
        assert list(reader[::-10]) == values[::-10]
        assert list(reader[::10]) == values[::10]
        assert list(reader[5:0:1]) == values[5:0:1]
        assert list(reader[0:5:-1]) == values[0:5:-1]
        assert list(reader[3:3:1]) == values[3:3:1]
        assert list(reader[3:3:-1]) == values[3:3:-1]
        assert list(reader[:8]) == values[:8]
        assert list(reader[-8:]) == values[-8:]
        assert list(reader[-8:8]) == values[-8:8]
        assert list(reader[-8:8:2]) == values[-8:8:2]

        # 切片的切片
        assert list(reader[:][:]) == values[:]
        assert list(reader[:][1:]) == values[1:]
        assert list(reader[1:][:1]) == values[1:2]
        assert list(reader[1:][::-1]) == values[:0:-1]
        assert reader[:8][-1] == values[-1]


def test_indexed_txt_reader_cross_iter(txt_stream):
    reader = IndexedTxtReader(txt_stream)
    iter1 = iter(reader)
    iter2 = iter(reader)
    item1 = next(iter1)
    result = [item1]
    assert item1 == next(iter2)

    for item1, item2 in zip(iter1, iter2):
        assert item1 == item2
        result.append(item1)

    assert len(result) == 6
    assert result == values


def test_indexed_txt_reader_with_index(fs, txt_stream):
    # Calculate offsets for txt data
    offsets = []
    offset = 0
    for value in values:
        offsets.append(offset)
        offset += len(value.encode("utf-8")) + 1  # +1 for newline

    index_file_path = "test.txt%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_stream:
        index_stream.write(generate_index_header(size=offset))
        for off in offsets:
            index_stream.write(struct.pack(INDEX_FILE_FORMAT, off))

    reader = IndexedTxtReader(txt_stream, index_file_path)
    assert list(reader._offsets) == offsets
    assert list((val for val in reader)) == values
    reader.close()


def test_indexed_txt_reader_with_wrong_index(fs, txt_stream):
    # Calculate offsets for txt data
    offsets = []
    offset = 0
    for value in values:
        offsets.append(offset)
        offset += len(value.encode("utf-8")) + 1  # +1 for newline

    index_file_path = "test.txt%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_stream:
        index_stream.write(generate_index_header(size=offset + 1))  # wrong size
        for off in offsets:
            index_stream.write(struct.pack(INDEX_FILE_FORMAT, off))

    reader = IndexedTxtReader(txt_stream, index_file_path)
    assert list(reader._offsets) == offsets
    assert list((val for val in reader)) == values
    reader.close()
    with open(index_file_path, "rb") as index_stream:
        assert index_stream.read()[:INDEX_FILE_HEADER_SIZE] == generate_index_header(
            size=offset
        )


def test_indexed_txt_reader_read_invalid(fs):
    txt_stream = BytesIO(b"")
    with IndexedTxtReader(txt_stream) as reader:
        list(reader)

    txt_stream = BytesIO(b"")
    index_file_path = "test.txt%s" % INDEX_FILE_POSTFIX
    with IndexedTxtReader(txt_stream, index_file_path) as reader:
        list(reader)

    txt_stream = BytesIO(b"")
    index_file_path = "test.txt%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_writer:
        index_writer.write(b"invalid")
    with IndexedTxtReader(txt_stream, index_file_path) as reader:
        pass
    with open(index_file_path, "rb") as index_reader:
        assert index_reader.read()[:INDEX_FILE_HEADER_SIZE] == EMPTY_INDEX_CONTENT


def test_indexed_txt_reader_with_invalid_indicated_index(fs, txt_stream):
    # Calculate offsets for txt data
    offset = 0
    for value in values:
        offset += len(value.encode("utf-8")) + 1

    # Rebuild index file, when index file is invalid
    index_file_path = "test.txt%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_writer:
        index_writer.write(struct.pack(INDEX_FILE_FORMAT, 100))
    with IndexedTxtReader(
        txt_stream, index_file_path, close_fileobj_when_close=True
    ) as reader:
        assert len(reader) == 6
    assert txt_stream.closed
    with open(index_file_path, "rb") as index_reader:
        content = index_reader.read()
        assert content[:INDEX_FILE_HEADER_SIZE] == generate_index_header(size=offset)


def test_indexed_txt_reader_close_after_exit(txt_stream):
    with IndexedTxtReader(
        txt_stream,
        close_fileobj_when_close=False,
    ) as reader:
        pass
    assert not txt_stream.closed
    reader.close()
    assert not txt_stream.closed


def test_indexed_txt_reader_auto_close(txt_stream):
    with IndexedTxtReader(txt_stream, close_fileobj_when_close=True):
        pass
    assert txt_stream.closed


def test_indexed_txt_reader_close_before_exit(txt_stream):
    with IndexedTxtReader(
        txt_stream,
        close_fileobj_when_close=False,
    ) as reader:
        reader.close()
    assert not txt_stream.closed


def test_indexed_txt_reader_with_multiple_close(txt_stream):
    with IndexedTxtReader(txt_stream, close_fileobj_when_close=True) as reader:
        reader.close()
        assert txt_stream.closed
    assert txt_stream.closed


def test_indexed_txt_open_fix_index(fs):
    with open("src.txt", "wb"):
        pass
    handler = indexed_txt_open("src.txt", mode="r")
    with open("src.txt%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT
    handler.close()


def test_indexed_txt_open_fix_index_2(fs):
    with open("src.txt", "wb") as txt_stream:
        txt_stream.write(b"line1\nline2\n")
    handler = indexed_txt_open("src.txt", mode="r")
    with open("src.txt%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        content = index_stream.read()
        assert (
            content[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00"
        )
        assert content[:INDEX_FILE_HEADER_SIZE] == generate_index_header(12)
    handler.close()


def test_indexed_txt_open(fs):
    handler = indexed_txt_open("src.txt", mode="w")
    assert type(handler) is IndexedTxtWriter
    assert handler.name == "src.txt"
    assert handler.mode == "w"

    handler = indexed_txt_open("src.txt", mode="a")
    assert type(handler) is IndexedTxtWriter
    assert handler.name == "src.txt"
    assert handler.mode == "a"

    handler = indexed_txt_open("src.txt", mode="r")
    assert type(handler) is IndexedTxtReader
    assert handler.name == "src.txt"
    assert handler.mode == "r"

    with pytest.raises(ValueError) as error:
        handler = indexed_txt_open("bad-mode.txt", mode="unknow mode")
    assert "unacceptable mode: 'unknow mode'" == str(error.value)


def test_indexed_txt_reader_with_errors_param(fs):
    # Test with invalid UTF-8 bytes and errors='replace'
    txt_stream = BytesIO(b"valid line\n\xff\xfe invalid bytes\n")
    with IndexedTxtReader(txt_stream, errors="replace") as reader:
        assert len(reader) == 2
        assert reader[0] == "valid line"
        # Invalid bytes should be replaced with replacement character
        assert "\ufffd" in reader[1]


def test_indexed_txt_reader_with_errors_strict(fs):
    # Test with invalid UTF-8 bytes and errors='strict' (default)
    txt_stream = BytesIO(b"valid line\n\xff\xfe invalid bytes\n")
    with IndexedTxtReader(txt_stream, errors="strict") as reader:
        assert len(reader) == 2
        assert reader[0] == "valid line"
        with pytest.raises(UnicodeDecodeError):
            reader[1]


def test_indexed_txt_reader_with_errors_ignore(fs):
    # Test with invalid UTF-8 bytes and errors='ignore'
    txt_stream = BytesIO(b"valid line\n\xff\xfe invalid bytes\n")
    with IndexedTxtReader(txt_stream, errors="ignore") as reader:
        assert len(reader) == 2
        assert reader[0] == "valid line"
        # Invalid bytes should be ignored
        assert reader[1] == " invalid bytes"


def test_indexed_txt_open_with_errors_param(fs):
    with open("src.txt", "wb") as txt_stream:
        txt_stream.write(b"valid line\n\xff\xfe invalid bytes\n")

    handler = indexed_txt_open("src.txt", mode="r", errors="replace")
    assert isinstance(handler, IndexedTxtReader)
    assert handler[0] == "valid line"
    assert "\ufffd" in handler[1]
    handler.close()


def test_indexed_txt_reader_with_index_build_callback(fs):
    txt_stream = BytesIO()
    for value in values:
        txt_stream.write(value.encode("utf-8"))
        txt_stream.write(b"\n")
    txt_stream.seek(0)

    callback_results = []

    def callback(line):
        callback_results.append(line)

    with IndexedTxtReader(txt_stream, index_build_callback=callback) as reader:
        assert len(reader) == len(values)

    assert len(callback_results) == len(values)
    assert callback_results[0] == b"line1\n"


def test_indexed_txt_open_with_index_build_callback(fs):
    with open("src.txt", "wb") as txt_stream:
        txt_stream.write(b"line1\nline2\n")

    callback_results = []

    def callback(line):
        callback_results.append(line)

    handler = indexed_txt_open("src.txt", mode="r", index_build_callback=callback)
    assert len(handler) == 2
    handler.close()

    assert len(callback_results) == 2


def test_indexed_txt_reader_batch_get_out_of_data(fs):
    # Create a txt file with corrupted content (offset points to wrong position)
    txt_stream = BytesIO(b"line1\nline2\n")

    with IndexedTxtReader(txt_stream) as reader:
        assert len(reader) == 2
        # This should work normally
        assert list(reader[0:2]) == ["line1", "line2"]


def test_indexed_txt_reader_get_out_of_data(fs):
    # Test the ValueError when reading out of data
    txt_stream = BytesIO(b"line1\nline2\n")

    with IndexedTxtReader(txt_stream) as reader:
        # Manually set wrong offset to trigger error
        reader._offsets[1] = 100  # Invalid offset
        with pytest.raises(ValueError) as error:
            reader[1]
        assert "out of data" in str(error.value)

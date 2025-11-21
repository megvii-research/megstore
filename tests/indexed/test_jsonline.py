import random
import struct
from io import BytesIO
from typing import List

import boto3
import orjson as json
import pytest
from megfile import smart_open
from megfile.utils import get_content_size
from moto import mock_aws as mock_s3
from pyfakefs.fake_filesystem_unittest import Patcher

from megstore.indexed.base import (
    INDEX_FILE_FORMAT,
    INDEX_FILE_HEADER_FORMAT,
    INDEX_FILE_POSTFIX,
)
from megstore.indexed.jsonline import (
    IndexedJsonlineReader,
    IndexedJsonlineWriter,
    indexed_jsonline_open,
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
    0,  # 5B
    1.5,  # 12B
    "string",  # 16B
    (1, 2, 3),  # 12B
    [1, 2, 3],  # 14B
    {  # 38B
        "1": 1,
        "2": 2,
        "3": 3,
    },
]

expected = [list(value) if isinstance(value, tuple) else value for value in values]


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


@mock_s3
def test_indexed_jsonline_s3_write(mocker):
    client = boto3.client("s3")
    client.create_bucket(Bucket="bucket")
    mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
    with Patcher():
        writer = indexed_jsonline_open(
            "s3://bucket/key.json",
            mode="w",
            index_path="s3://bucket/key.json%s" % INDEX_FILE_POSTFIX,
        )
        assert isinstance(writer, IndexedJsonlineWriter)

        writer.append(2)
        writer.close()

    assert client.get_object(Bucket="bucket", Key="key.json")["Body"].read() == b"2\n"


def test_indexed_jsonline_writer(fs):
    jsonline_stream = BytesIO()
    index_path = "key.idx"
    with IndexedJsonlineWriter(
        jsonline_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        assert writer._file_object.tell() == 0
        writer.append(values[0])  # 增加一个 value: 0，占 5B
        assert writer._file_object.tell() == 2
        assert jsonline_stream.getvalue()[-1:] == b"\n"

        writer.extend(values[1:])
        assert writer._file_object.tell() == 51

        assert jsonline_stream.getvalue().strip() == b"\n".join(
            [
                json.dumps(0),
                json.dumps(1.5),
                json.dumps("string"),
                json.dumps((1, 2, 3)),
                json.dumps([1, 2, 3]),
                json.dumps({"1": 1, "2": 2, "3": 3}),
            ]
        )

        writer.commit()
        assert jsonline_stream.tell() == 51

    assert jsonline_stream.closed
    with smart_open(index_path, "rb") as index_reader:
        assert get_content_size(index_reader) == 6 * 8 + INDEX_FILE_HEADER_SIZE
        assert unpack_indexes(index_reader.read()) == [0, 2, 6, 15, 23, 31]


def test_indexed_jsonline_writer_with_context_manager_without_close(fs):
    jsonline_stream = BytesIO()
    index_path = "key.idx"
    with IndexedJsonlineWriter(
        jsonline_stream, index_path, close_fileobj_when_close=False
    ):
        pass
    assert not jsonline_stream.closed
    assert jsonline_stream.getvalue() == b""
    jsonline_stream.close()
    with smart_open(index_path, "rb") as index_reader:
        assert index_reader.read() == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_writer_with_context_manager_auto_close(fs):
    jsonline_stream = open("auto_close.json", "wb")
    index_path = "auto_close.json%s" % INDEX_FILE_POSTFIX
    with IndexedJsonlineWriter(
        jsonline_stream, index_path, close_fileobj_when_close=True
    ):
        pass
    assert jsonline_stream.closed

    with open("auto_close.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_writer_with_context_manager_do_close_in_context(fs):
    jsonline_stream = open("file.json", "wb")
    index_path = "file.json%s" % INDEX_FILE_POSTFIX
    with IndexedJsonlineWriter(
        jsonline_stream,
        index_path,
        close_fileobj_when_close=False,
    ) as writer:
        writer.close()
    assert not jsonline_stream.closed

    jsonline_stream.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_writer_with_context_manager_do_close_after_context(fs):
    jsonline_stream = open("file.json", "wb")
    index_path = "file.json%s" % INDEX_FILE_POSTFIX
    with IndexedJsonlineWriter(
        jsonline_stream,
        index_path,
        close_fileobj_when_close=False,
    ) as writer:
        pass
    assert not jsonline_stream.closed

    writer.close()
    assert not jsonline_stream.closed

    jsonline_stream.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_writer_with_context_manager_do_multiple_close(fs):
    jsonline_stream = open("file.json", "wb")
    index_path = "file.json%s" % INDEX_FILE_POSTFIX
    with IndexedJsonlineWriter(
        jsonline_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        writer.close()
        assert jsonline_stream.closed
    writer.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_writer_without_context_manager(fs):
    jsonline_stream = open("file.json", "wb")
    index_path = "file.json%s" % INDEX_FILE_POSTFIX
    writer = IndexedJsonlineWriter(jsonline_stream, index_path)
    writer.commit()

    assert not jsonline_stream.closed

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b""
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT

    writer.append(1)

    # 多次 commit 和 close
    writer.commit()
    writer.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b"1\n"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00"
        )


def test_indexed_jsonline_writer_append_mode(fs):
    with indexed_jsonline_open("file.json", "a") as writer:
        writer.append(1)

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b"1\n"
    with open("file.json%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00"
        )

    with indexed_jsonline_open("file.json", "a") as writer:
        writer.append(2)
    jsonline_stream.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b"1\n2\n"
    with open("file.json%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00"
        )

    with indexed_jsonline_open("file.json", "w") as writer:
        writer.append(3)
    jsonline_stream.close()

    with open("file.json", "rb") as jsonline_stream:
        assert jsonline_stream.read() == b"3\n"
    with open("file.json%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00"
        )


@pytest.fixture
def jsonline_stream():
    jsonline_stream = BytesIO()
    for value in values:
        jsonline_stream.write(json.dumps(value))
        jsonline_stream.write(b"\n")
    jsonline_stream.seek(0)
    yield jsonline_stream


def test_indexed_jsonline_reader_without_index(jsonline_stream):
    with IndexedJsonlineReader(jsonline_stream) as reader:
        assert len(reader) == 6
        assert list(reader._offsets) == [0, 2, 6, 15, 23, 31]

        # 随机读
        assert reader[0] == 0
        assert reader[4] == [1, 2, 3]
        assert reader[1] == 1.5
        assert reader[2] == "string"
        assert reader[5] == {
            "1": 1,
            "2": 2,
            "3": 3,
        }
        assert reader[3] == [1, 2, 3]
        assert reader[-1] == {
            "1": 1,
            "2": 2,
            "3": 3,
        }
        assert reader[-4] == "string"

        for _ in range(100):
            index = random.randint(-6, 5)
            assert expected[index] == reader[index]

        # 越界读
        with pytest.raises(IndexError) as error:
            reader[-7]
        assert repr(jsonline_stream) in str(error.value)

        with pytest.raises(IndexError) as error:
            reader[6]
        assert repr(jsonline_stream) in str(error.value)

        # scan, iter
        assert [x for x in reader] == expected


def test_indexed_jsonline_reader_read_by_slice(jsonline_stream):
    with IndexedJsonlineReader(jsonline_stream) as reader:
        # 切片
        assert list(reader[:]) == expected[:]
        assert list(reader[::]) == expected[::]
        assert list(reader[1:]) == expected[1:]
        assert list(reader[:-1]) == expected[:-1]
        assert list(reader[::-1]) == expected[::-1]
        assert list(reader[::-2]) == expected[::-2]
        assert list(reader[::-10]) == expected[::-10]
        assert list(reader[::10]) == expected[::10]
        assert list(reader[5:0:1]) == expected[5:0:1]
        assert list(reader[0:5:-1]) == expected[0:5:-1]
        assert list(reader[3:3:1]) == expected[3:3:1]
        assert list(reader[3:3:-1]) == expected[3:3:-1]
        assert list(reader[:8]) == expected[:8]
        assert list(reader[-8:]) == expected[-8:]
        assert list(reader[-8:8]) == expected[-8:8]
        assert list(reader[-8:8:2]) == expected[-8:8:2]

        # 切片的切片
        assert list(reader[:][:]) == expected[:]
        assert list(reader[:][1:]) == expected[1:]
        assert list(reader[1:][:1]) == expected[1:2]
        assert list(reader[1:][::-1]) == expected[:0:-1]
        assert reader[:8][-1] == expected[-1]


def test_indexed_msgpack_reader_cross_iter(jsonline_stream):
    reader = IndexedJsonlineReader(jsonline_stream)
    iter1 = iter(reader)
    iter2 = iter(reader)
    item1 = next(iter1)
    result = [item1]
    assert item1 == next(iter2)
    # shadow_copy 使用 BufferedReader 后 tell 改变了
    assert jsonline_stream.tell() == 2  # read_size

    for item1, item2 in zip(iter1, iter2):
        assert item1 == item2
        result.append(item1)

    assert len(result) == 6
    assert result == expected


def test_indexed_jsonline_reader_with_index(fs, jsonline_stream):
    index_file_path = "test.json%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_stream:
        index_stream.write(generate_index_header(size=51))
        for offset in [0, 2, 6, 15, 23, 31]:
            index_stream.write(struct.pack(INDEX_FILE_FORMAT, offset))

    reader = IndexedJsonlineReader(jsonline_stream, index_file_path)
    assert list(reader._offsets) == [0, 2, 6, 15, 23, 31]
    assert list((val for val in reader)) == expected
    reader.close()


def test_indexed_jsonline_reader_with_wrong_index(fs, jsonline_stream):
    index_file_path = "test.json%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_stream:
        index_stream.write(generate_index_header(size=52))
        for offset in [0, 2, 6, 15, 23, 31]:
            index_stream.write(struct.pack(INDEX_FILE_FORMAT, offset))

    reader = IndexedJsonlineReader(jsonline_stream, index_file_path)
    assert list(reader._offsets) == [0, 2, 6, 15, 23, 31]
    assert list((val for val in reader)) == expected
    reader.close()
    with open(index_file_path, "rb") as index_stream:
        assert index_stream.read()[:INDEX_FILE_HEADER_SIZE] == generate_index_header(
            size=51
        )


def test_indexed_jsonline_reader_read_invalid(fs):
    jsonline_stream = BytesIO(b"")
    with IndexedJsonlineReader(jsonline_stream) as reader:
        list(reader)

    jsonline_stream = BytesIO(b"")
    index_file_path = "test.json%s" % INDEX_FILE_POSTFIX
    with IndexedJsonlineReader(jsonline_stream, index_file_path) as reader:
        list(reader)
    # jsonline_stream = BytesIO(b'invalid')
    # with pytest.raises(Exception) as error:
    #     with IndexedJsonlineReader(jsonline_stream) as reader:
    #         pass

    jsonline_stream = BytesIO(b"")
    index_file_path = "test.json%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_writer:
        index_writer.write(b"invalid")
    with IndexedJsonlineReader(jsonline_stream, index_file_path) as reader:
        pass
    with open(index_file_path, "rb") as index_reader:
        assert index_reader.read()[:INDEX_FILE_HEADER_SIZE] == EMPTY_INDEX_CONTENT


def test_indexed_jsonline_reader_with_invalid_indicated_index(fs, jsonline_stream):
    # 错误 index 文件，重建
    index_file_path = "test.json%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_writer:
        index_writer.write(struct.pack(INDEX_FILE_FORMAT, 100))
    with IndexedJsonlineReader(
        jsonline_stream, index_file_path, close_fileobj_when_close=True
    ) as reader:
        assert len(reader) == 6
    assert jsonline_stream.closed
    with open(index_file_path, "rb") as index_reader:
        content = index_reader.read()
        assert content[:INDEX_FILE_HEADER_SIZE] == generate_index_header(size=51)
        assert unpack_indexes(content) == [0, 2, 6, 15, 23, 31]


def test_indexed_jsonline_reader_close_after_exit(jsonline_stream):
    with IndexedJsonlineReader(
        jsonline_stream,
        close_fileobj_when_close=False,
    ) as reader:
        pass
    assert not jsonline_stream.closed
    reader.close()
    assert not jsonline_stream.closed


def test_indexed_jsonline_reader_auto_close(jsonline_stream):
    with IndexedJsonlineReader(jsonline_stream, close_fileobj_when_close=True):
        pass
    assert jsonline_stream.closed


def test_indexed_jsonline_reader_close_before_exit(jsonline_stream):
    with IndexedJsonlineReader(
        jsonline_stream,
        close_fileobj_when_close=False,
    ) as reader:
        reader.close()
    assert not jsonline_stream.closed


def test_indexed_jsonline_reader_with_multiple_close(jsonline_stream):
    with IndexedJsonlineReader(
        jsonline_stream, close_fileobj_when_close=True
    ) as reader:
        reader.close()
        assert jsonline_stream.closed


def test_indexed_jsonline_open_fix_index(fs):
    with open("src.json", "wb"):
        pass
    handler = indexed_jsonline_open("src.json", mode="r")
    with open("src.json%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT
    handler.close()


def test_indexed_jsonline_open_fix_index_2(fs):
    with open("src.json", "wb") as jsonline_stream:
        jsonline_stream.write(b"1\n2\n")
    handler = indexed_jsonline_open("src.json", mode="r")
    with open("src.json%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        content = index_stream.read()
        assert (
            content[INDEX_FILE_HEADER_SIZE:]
            == b"\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00"
        )
        assert content[:INDEX_FILE_HEADER_SIZE] == generate_index_header(4)
    handler.close()


def test_indexed_jsonline_open(fs):
    handler = indexed_jsonline_open("src.json", mode="w")
    assert type(handler) is IndexedJsonlineWriter
    assert handler.name == "src.json"
    assert handler.mode == "w"

    handler = indexed_jsonline_open("src.json", mode="a")
    assert type(handler) is IndexedJsonlineWriter
    assert handler.name == "src.json"
    assert handler.mode == "a"

    handler = indexed_jsonline_open("src.json", mode="r")
    assert type(handler) is IndexedJsonlineReader
    assert handler.name == "src.json"
    assert handler.mode == "r"

    with pytest.raises(ValueError) as error:
        handler = indexed_jsonline_open("bad-mode.msg", mode="unknow mode")
    assert "unacceptable mode: 'unknow mode'" == str(error.value)

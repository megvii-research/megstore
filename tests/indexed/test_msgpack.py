import random
import struct
from io import BytesIO
from typing import List

import boto3
import msgpack
import pytest
from megfile.smart import smart_open
from moto import mock_aws as mock_s3
from pyfakefs.fake_filesystem_unittest import Patcher

from megstore.indexed.base import (
    INDEX_FILE_FORMAT,
    INDEX_FILE_HEADER_FORMAT,
    INDEX_FILE_POSTFIX,
)
from megstore.indexed.msgpack import (
    IndexedMsgpackHandler,
    IndexedMsgpackReader,
    IndexedMsgpackWriter,
    indexed_msgpack_open,
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
    0,  # 1B
    1.5,  # 9B
    "string",  # 7B
    (1, 2, 3),  # 4B
    [1, 2, 3],  # 4B
    {  # 10B
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
    for i in range(len(data) // INDEX_FILE_RECROD_SIZE):
        offset_bytes = data[
            i * INDEX_FILE_RECROD_SIZE : (i + 1) * INDEX_FILE_RECROD_SIZE
        ]
        offset = struct.unpack(INDEX_FILE_FORMAT, offset_bytes)[0]
        offsets.append(offset)
    return offsets


@mock_s3
def test_indexed_msgpack_s3_write(mocker):
    client = boto3.client("s3")
    client.create_bucket(Bucket="bucket")
    mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
    with Patcher():
        writer = indexed_msgpack_open(
            "s3://bucket/key.msg",
            mode="w",
            index_path="s3://bucket/key.msg%s" % INDEX_FILE_POSTFIX,
        )
        assert isinstance(writer, IndexedMsgpackWriter)

        writer.append(2)
        writer.close()

    assert (
        client.get_object(Bucket="bucket", Key="key.msg")["Body"].read()
        == b"\xdd\x00\x00\x00\x01\x02"
    )


def test_indexed_msgpack_writer(fs):
    msgpack_stream = BytesIO()
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(
        msgpack_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        assert writer._file_object.tell() == 5
        writer.append(values[0])  # 增加一个 value: 0，占 1B
        assert writer._file_object.tell() == 6
        assert msgpack_stream.getvalue()[-1:] == b"\x00"

        # 增加剩余的 5 个 value，共占 9 + 7 + 4 + 4 + 10 = 34B
        writer.extend(values[1:])
        assert writer._file_object.tell() == 40

        packer = msgpack.Packer()
        assert msgpack_stream.getvalue()[5:] == b"".join(
            [
                packer.pack(0),
                packer.pack(1.5),
                packer.pack("string"),
                packer.pack([1, 2, 3]),
                packer.pack((1, 2, 3)),
                packer.pack({"1": 1, "2": 2, "3": 3}),
            ]
        )

        writer.commit()
        # header: 5B + 各个 value 大小
        assert msgpack_stream.tell() == 5 + 1 + 9 + 7 + 4 + 4 + 10
        # 一个 8B，共 6 个

    assert msgpack_stream.closed
    with open(index_path, "rb") as index_stream:
        assert unpack_indexes(index_stream.read()[INDEX_FILE_HEADER_SIZE:]) == [
            5,
            6,
            15,
            22,
            26,
            30,
        ]
        assert index_stream.tell() == 6 * 8 + INDEX_FILE_HEADER_SIZE


def test_indexed_msgpack_writer_tell(fs):
    msgpack_stream = BytesIO()
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(msgpack_stream, index_path) as writer:
        assert writer.tell() == 0
        writer.append(0)
        assert writer.tell() == 1
        writer.extend([0, 0])
        assert writer.tell() == 3
    msgpack_stream.close()


def test_indexed_msgpack_writer_with_context_manager_without_close(fs):
    msgpack_stream = BytesIO()
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(msgpack_stream, index_path):
        pass
    assert not msgpack_stream.closed
    assert msgpack_stream.getvalue() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == generate_index_header(5)
    msgpack_stream.close()


def test_indexed_msgpack_writer_with_context_manager_auto_close(fs):
    msgpack_stream = open("auto_close.msg", "wb")
    index_path = "auto_close.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(
        msgpack_stream, index_path, close_fileobj_when_close=True
    ):
        pass
    assert msgpack_stream.closed

    with open("auto_close.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == generate_index_header(5)


def test_indexed_msgpack_writer_with_context_manager_do_close_in_context(fs):
    msgpack_stream = open("file.msg", "wb")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(msgpack_stream, index_path) as writer:
        writer.close()
    assert not msgpack_stream.closed

    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == generate_index_header(5)


def test_indexed_msgpack_writer_with_context_manager_do_close_after_context(fs):
    msgpack_stream = open("file.msg", "wb")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(msgpack_stream, index_path) as writer:
        pass
    assert not msgpack_stream.closed

    writer.close()
    assert not msgpack_stream.closed

    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == generate_index_header(5)


def test_indexed_msgpack_writer_with_context_manager_do_multiple_close(fs):
    msgpack_stream = open("file.msg", "wb")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackWriter(
        msgpack_stream, index_path, close_fileobj_when_close=True
    ) as writer:
        writer.close()
        assert msgpack_stream.closed
    writer.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == generate_index_header(5)


def test_indexed_msgpack_writer_without_context_manager(fs):
    msgpack_stream = open("file.msg", "wb")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    writer = IndexedMsgpackWriter(msgpack_stream, index_path)
    writer.commit()

    assert not msgpack_stream.closed

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x00"
    with open(index_path, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT

    writer.append(1)

    # 多次 commit 和 close
    writer.commit()
    writer.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x01"
    with open(index_path, "rb") as index_stream:
        content = index_stream.read()
        assert content[INDEX_FILE_HEADER_SIZE:] == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        assert content[:INDEX_FILE_HEADER_SIZE] == generate_index_header(6)


def test_indexed_msgpack_writer_append_mode(fs):
    msgpack_stream = open("file.msg", "wb")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    writer = IndexedMsgpackWriter(msgpack_stream, index_path)
    writer.append(1)
    assert writer.tell() == 1
    writer.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x01"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        )

    msgpack_stream = open("file.msg", "rb+")
    writer = IndexedMsgpackWriter(msgpack_stream, index_path, append_mode=True)
    assert writer.tell() == 1
    writer.append(2)
    assert writer.tell() == 2
    writer.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x02\x01\x02"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00"
        )

    msgpack_stream = open("file.msg", "wb+")
    writer = IndexedMsgpackWriter(msgpack_stream, index_path, append_mode=False)
    assert writer.tell() == 0
    writer.append(3)
    assert writer.tell() == 1
    writer.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x03"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        )


def test_indexed_msgpack_writer_append_mode_2(fs):
    writer = indexed_msgpack_open("file.msg", "a")
    writer.append(1)
    assert writer.tell() == 1
    writer.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x01"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        )

    writer = indexed_msgpack_open("file.msg", "a")
    assert writer.tell() == 1
    writer.append(2)
    assert writer.tell() == 2
    writer.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x02\x01\x02"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00"
        )

    writer = indexed_msgpack_open("file.msg", "w")
    assert writer.tell() == 0
    writer.append(3)
    assert writer.tell() == 1
    writer.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x03"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        )


@pytest.fixture
def msgpack_stream():
    msgpack_stream = BytesIO()
    msgpack_stream.write(b"\xdd")
    msgpack_stream.write(b"\x00\x00\x00\x06")
    for value in values:
        msgpack_stream.write(msgpack.packb(value))
    msgpack_stream.seek(0)
    yield msgpack_stream


def test_indexed_msgpack_reader_without_index(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream) as reader:
        assert len(reader) == 6
        assert list(reader._offsets) == [5, 6, 15, 22, 26, 30]

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
            if values[index] == (1, 2, 3):
                assert list(values[index]) == reader[index]
            else:
                assert values[index] == reader[index]

        # 越界读
        with pytest.raises(IndexError) as error:
            reader[-7]
        assert repr(msgpack_stream) in str(error.value)

        with pytest.raises(IndexError) as error:
            reader[6]
        assert repr(msgpack_stream) in str(error.value)

        # scan, iter
        assert [x for x in reader] == expected


def test_indexed_msgpack_reader_read_by_slice(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream) as reader:
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


def test_indexed_msgpack_reader_cross_iter(mocker):
    def fake_unpacker(file_like, **kwargs):
        return msgpack.Unpacker(file_like, read_size=128, **kwargs)

    mocker.patch("megstore.utils.compat_msgpack.Unpacker", side_effect=fake_unpacker)

    data = msgpack.packb(list(range(1024)))

    msgpack_stream = BytesIO(data)
    read_func = mocker.patch.object(
        msgpack_stream, "read", side_effect=msgpack_stream.read
    )

    reader = IndexedMsgpackReader(msgpack_stream)

    msgpack_stream.seek(0)
    iter1 = iter(reader)
    iter2 = iter(reader)
    item1 = next(iter1)
    result = [item1]
    assert item1 == next(iter2)
    assert read_func.call_count > 0
    assert msgpack_stream.tell() == 131
    read_func.reset_mock()

    for item1, item2 in zip(iter1, iter2):
        assert item1 == item2
        result.append(item1)

    assert len(result) == 1024
    assert result == list(range(1024))
    assert read_func.call_count > 0


def test_indexed_msgpack_reader_with_index(msgpack_stream):
    reader = IndexedMsgpackReader(msgpack_stream)
    assert list(reader._offsets) == [5, 6, 15, 22, 26, 30]
    assert list((val for val in reader)) == expected
    reader.close()


def test_indexed_msgpack_reader_with_non_array32_header():
    # 非 msgpack array32 header，实际是 msgpack fixarray: \x90~f
    # 第一个字节高 4 位为 1001，低 4 位记录 array 长，所以这里第一个字节为 \x96
    # 第一个 value 的 offset 为 1
    msgpack_stream = BytesIO(msgpack.packb(values))
    reader = IndexedMsgpackReader(msgpack_stream)
    assert list(reader._offsets) == [1, 2, 11, 18, 22, 26]
    # 随机读
    for _ in range(100):
        index = random.randint(-6, 5)
        if values[index] == (1, 2, 3):
            assert list(values[index]) == reader[index]
        else:
            assert values[index] == reader[index]
    reader.close()


def test_indexed_msgpack_reader_read_invalid(fs):
    msgpack_stream = BytesIO(b"")
    with IndexedMsgpackReader(msgpack_stream) as reader:
        list(reader)

    msgpack_stream = BytesIO(b"")
    index_file_path = "test.msg%s" % INDEX_FILE_POSTFIX
    with IndexedMsgpackReader(msgpack_stream, index_file_path) as reader:
        list(reader)

    with open("test_msgpack", "wb") as writer:
        writer.write(b"\x00")
    with smart_open("test_msgpack", "rb") as msgpack_stream:
        with pytest.raises(Exception) as error:
            with IndexedMsgpackReader(msgpack_stream) as reader:
                pass
        assert "test_msgpack" in str(error.value)


def test_indexed_msgpack_reader_with_invalid_array_header(fs):
    # 无效 array header，实际是 fixmap
    with open("test_msgpack.msg", "wb") as writer:
        writer.write(msgpack.packb({"1": 1, "2": 2, "3": 3}))
    with smart_open("test_msgpack.msg", "rb") as msgpack_stream:
        with pytest.raises(ValueError) as error:
            IndexedMsgpackReader(msgpack_stream)
        assert "test_msgpack.msg" in str(error.value)


def test_indexed_msgpack_reader_with_over_length_header():
    # array header 长度超过实际长度
    msgpack_stream = BytesIO(msgpack.packb(values))
    msgpack_stream.write(
        b"\x97"
    )  # \x90~f，msgpack fixarr flag，7 表示 array 长为 7，实际长度为 6
    msgpack_stream.seek(0)
    reader = IndexedMsgpackReader(msgpack_stream)
    assert len(reader) == 6


def test_indexed_msgpack_reader_with_invalid_indicated_index(fs, msgpack_stream):
    # 错误 index 文件，重建
    index_file_path = "test.msg%s" % INDEX_FILE_POSTFIX
    with open(index_file_path, "wb") as index_writer:
        index_writer.write(struct.pack(INDEX_FILE_FORMAT, 100))
    with IndexedMsgpackReader(
        msgpack_stream, index_file_path, close_fileobj_when_close=True
    ) as reader:
        assert len(reader) == 6
    assert msgpack_stream.closed


def test_indexed_msgpack_reader_close_after_exit(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream) as reader:
        pass
    assert not msgpack_stream.closed
    reader.close()
    assert not msgpack_stream.closed


def test_indexed_msgpack_reader_auto_close(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream, close_fileobj_when_close=True):
        pass
    assert msgpack_stream.closed


def test_indexed_msgpack_reader_close_before_exit(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream) as reader:
        reader.close()
    assert not msgpack_stream.closed


def test_indexed_msgpack_reader_with_multiple_close(msgpack_stream):
    with IndexedMsgpackReader(msgpack_stream, close_fileobj_when_close=True) as reader:
        reader.close()
        assert msgpack_stream.closed


def test_indexed_msgpack_handler_with_index(fs):
    msgpack_stream = open("file.msg", "wb+")
    handler = IndexedMsgpackHandler(msgpack_stream)

    handler.append(1)
    assert len(handler) == 1
    handler.append(2)
    assert len(handler) == 2

    assert handler[0] == 1
    handler.append(3)
    assert len(handler) == 3

    assert handler[0] == 1
    assert handler[-1] == 3
    assert list(handler) == [1, 2, 3]
    assert list(handler[:]) == [1, 2, 3]
    assert list(handler[1:]) == [2, 3]
    assert list(handler[:2]) == [1, 2]

    handler.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x03\x01\x02\x03"

    msgpack_stream = open("file.msg", "rb+")
    handler = IndexedMsgpackHandler(msgpack_stream)
    assert len(handler) == 3
    handler.append(4)
    assert len(handler) == 4
    assert handler[0] == 1
    assert handler[-1] == 4
    assert list(handler) == [1, 2, 3, 4]
    assert list(handler[:]) == [1, 2, 3, 4]
    assert list(handler[1:]) == [2, 3, 4]
    assert list(handler[:3]) == [1, 2, 3]
    handler.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x04\x01\x02\x03\x04"


def test_indexed_msgpack_handler_without_index(fs):
    msgpack_stream = open("file.msg", "wb+")
    index_path = "file.msg%s" % INDEX_FILE_POSTFIX
    handler = IndexedMsgpackHandler(msgpack_stream, index_path)

    handler.append(1)
    assert len(handler) == 1
    handler.append(2)
    assert len(handler) == 2

    assert handler[0] == 1
    handler.append(3)
    assert len(handler) == 3

    assert handler[0] == 1
    assert handler[-1] == 3
    assert list(handler) == [1, 2, 3]
    assert list(handler[:]) == [1, 2, 3]
    assert list(handler[1:]) == [2, 3]
    assert list(handler[:2]) == [1, 2]

    handler.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x03\x01\x02\x03"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00"  # noqa: E501
        )

    msgpack_stream = open("file.msg", "rb+")
    handler = IndexedMsgpackHandler(msgpack_stream, index_path)
    assert len(handler) == 3
    handler.append(4)
    assert len(handler) == 4
    assert handler[0] == 1
    assert handler[-1] == 4
    assert list(handler) == [1, 2, 3, 4]
    assert list(handler[:]) == [1, 2, 3, 4]
    assert list(handler[1:]) == [2, 3, 4]
    assert list(handler[:3]) == [1, 2, 3]
    handler.close()
    msgpack_stream.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x04\x01\x02\x03\x04"
    with open(index_path, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00"  # noqa: E501
        )


def test_indexed_msgpack_handler_without_index_2(fs):
    handler = indexed_msgpack_open("file.msg", "w+")

    handler.append(1)
    assert len(handler) == 1
    handler.append(2)
    assert len(handler) == 2

    assert handler[0] == 1
    handler.append(3)
    assert len(handler) == 3

    assert handler[0] == 1
    assert handler[-1] == 3
    assert list(handler) == [1, 2, 3]
    assert list(handler[:]) == [1, 2, 3]
    assert list(handler[1:]) == [2, 3]
    assert list(handler[:2]) == [1, 2]

    handler.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x03\x01\x02\x03"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00"  # noqa: E501
        )

    handler = indexed_msgpack_open("file.msg", "a+")
    assert len(handler) == 3
    handler.append(4)
    assert len(handler) == 4
    assert handler[0] == 1
    assert handler[-1] == 4
    assert list(handler) == [1, 2, 3, 4]
    assert list(handler[:]) == [1, 2, 3, 4]
    assert list(handler[1:]) == [2, 3, 4]
    assert list(handler[:3]) == [1, 2, 3]
    handler.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x04\x01\x02\x03\x04"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00"  # noqa: E501
        )

    handler = indexed_msgpack_open("file.msg", "w+")

    handler.append(1)
    assert handler.count() == 1
    assert list(handler) == [1]
    handler.close()

    with open("file.msg", "rb") as msgpack_stream:
        assert msgpack_stream.read() == b"\xdd\x00\x00\x00\x01\x01"
    with open("file.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert (
            index_stream.read()[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00"
        )


def test_indexed_msgpack_handler_read_by_slice(msgpack_stream):
    with IndexedMsgpackHandler(msgpack_stream) as reader:
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


def test_indexed_msgpack_open_fix_index(fs):
    with open("src.msg", "wb"):
        pass
    with indexed_msgpack_open("src.msg", mode="r"):
        pass
    with open("src.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        assert index_stream.read() == EMPTY_INDEX_CONTENT


def test_indexed_msgpack_open_fix_index_2(fs):
    with open("src.msg", "wb") as msgpack_stream:
        msgpack_stream.write(b"\xdd\x00\x00\x00\x04\x01\x02\x03\x04")
    with indexed_msgpack_open("src.msg", mode="r"):
        pass
    with open("src.msg%s" % INDEX_FILE_POSTFIX, "rb") as index_stream:
        content = index_stream.read()
        assert (
            content[INDEX_FILE_HEADER_SIZE:]
            == b"\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00"  # noqa: E501
        )


def test_indexed_msgpack_open(fs):
    handler = indexed_msgpack_open("src.msg", mode="w")
    assert type(handler) is IndexedMsgpackWriter
    assert handler.name == "src.msg"
    assert handler.mode == "w"

    handler = indexed_msgpack_open("src.msg", mode="a")
    assert type(handler) is IndexedMsgpackWriter
    assert handler.name == "src.msg"
    assert handler.mode == "a"

    handler = indexed_msgpack_open("src.msg", mode="r")
    assert type(handler) is IndexedMsgpackReader
    assert handler.name == "src.msg"
    assert handler.mode == "r"

    handler = indexed_msgpack_open("src.msg", mode="a+")
    assert type(handler) is IndexedMsgpackHandler
    assert handler.name == "src.msg"
    assert handler.mode == "a+"

    handler = indexed_msgpack_open("src.msg", mode="w+")
    assert type(handler) is IndexedMsgpackHandler
    assert handler.name == "src.msg"
    assert handler.mode == "w+"

    with pytest.raises(ValueError) as error:
        handler = indexed_msgpack_open("bad-mode.msg", mode="unknow mode")
    assert "unacceptable mode: 'unknow mode'" == str(error.value)

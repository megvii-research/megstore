from fractions import Fraction
from io import BytesIO
from pathlib import Path

import boto3
import numpy as np
import pytest
from moto import mock_aws as mock_s3

import megstore.video as video_module
from megstore import VideoReader, VideoWriter, video_open
from megstore.video import (
    DEFAULT_VIDEO_READ_BUFFER_SIZE,
    VideoStreamInfo,
    _build_hwaccel,
    _ByteRange,
    _scan_mp4_top_level_boxes,
    _SparseContentReader,
)

av = pytest.importorskip("av")


def _build_local_video(
    path: Path,
    *,
    frame_count: int = 6,
    codec_name: str = "libx264",
    format_name: str | None = None,
    stream_options: dict[str, str] | None = None,
):
    """Create a deterministic sample video with PyAV.

    :param path: Output video path.
    :param frame_count: Number of frames to encode.
    :param codec_name: Encoder codec name.
    :param format_name: Optional container format name.
    :param stream_options: Optional stream options.
    """
    container = av.open(str(path), "w", format=format_name)
    stream = container.add_stream(codec_name, rate=5)
    stream.width = 16
    stream.height = 16
    stream.pix_fmt = "yuv420p"
    if stream_options:
        stream.options = stream_options

    for index in range(frame_count):
        array = np.full((16, 16, 3), index * 30, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(array, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)

    for packet in stream.encode(None):
        container.mux(packet)
    container.close()


def _decode_video(path: Path) -> list:
    """Decode all frames in a local video file.

    :param path: Local video path.
    :returns: List of decoded frames.
    """
    container = av.open(str(path), "r")
    try:
        return list(container.decode(video=0))
    finally:
        container.close()


class _TrackingBytesIO(BytesIO):
    """BytesIO variant that records uncached backend reads."""

    def __init__(self, data: bytes):
        super().__init__(data)
        self.read_calls: list[tuple[int, int]] = []

    def read(self, size: int = -1) -> bytes:
        """Read data and record the call.

        :param size: Read size.
        :returns: Read bytes.
        """
        self.read_calls.append((self.tell(), size))
        return super().read(size)


def test_sparse_content_reader():
    """The sparse reader should serve cached bytes and lazy-load cache misses."""
    content_path = Path("content.bin")
    backend = _TrackingBytesIO(b"0123456789abcdef")
    load_calls = []

    def _load_content(path: str, start: int | None = None, stop: int | None = None):
        """Read bytes from the tracking backend.

        :param path: Content path.
        :param start: Range start.
        :param stop: Range stop.
        :returns: Read bytes.
        """
        assert path == str(content_path)
        load_calls.append((start, stop))
        backend.seek(start or 0)
        return backend.read(-1 if stop is None else stop - (start or 0))

    reader = None
    original_loader = video_module.smart_load_content
    video_module.smart_load_content = _load_content
    try:
        reader = _SparseContentReader(
            str(content_path),
            size=16,
            block_size=4,
            preload_ranges=(
                _ByteRange(0, 4),
                _ByteRange(12, 16),
            ),
        )

        assert load_calls == [(0, 4), (12, 16)]
        backend.read_calls.clear()
        load_calls.clear()

        reader.seek(1)
        assert reader.read(2) == b"12"
        assert load_calls == []
        assert backend.read_calls == []

        reader.seek(13)
        assert reader.read(2) == b"de"
        assert load_calls == []
        assert backend.read_calls == []

        reader.seek(6)
        assert reader.read(2) == b"67"
        assert load_calls == [(4, 8)]
        assert backend.read_calls == [(4, 4)]
    finally:
        if reader is not None:
            reader.close()
        video_module.smart_load_content = original_loader


def test_video_reader_index_and_slice(tmp_path: Path):
    """VideoReader should support frame indexing and slicing."""
    video_path = tmp_path / "sample.mp4"
    _build_local_video(video_path, stream_options={"g": "2"})

    expected_frames = _decode_video(video_path)

    with video_open(str(video_path), "r") as reader:
        assert isinstance(reader, VideoReader)
        assert len(reader) == len(expected_frames)
        assert reader[3].pts == expected_frames[3].pts
        assert reader[-1].pts == expected_frames[-1].pts
        assert [frame.pts for frame in reader[1:4]] == [
            frame.pts for frame in expected_frames[1:4]
        ]
        assert [frame.pts for frame in reader[:]] == [
            frame.pts for frame in expected_frames
        ]

    with video_open(str(video_path), "r") as reader:
        assert reader[2].pts == expected_frames[2].pts

    assert not (tmp_path / "sample.mp4.vidx").exists()


def test_scan_mp4_top_level_boxes(tmp_path: Path):
    """MP4 box scanning should find top-level metadata and media boxes."""
    video_path = tmp_path / "sample.mp4"
    _build_local_video(video_path, codec_name="mpeg4")

    boxes = _scan_mp4_top_level_boxes(str(video_path), video_path.stat().st_size)
    box_types = [box.type_name for box in boxes]

    assert box_types[0] == "ftyp"
    assert "mdat" in box_types
    assert "moov" in box_types


def test_video_reader_mp4_single_get_uses_smart_load_content(
    tmp_path: Path,
    mocker,
):
    """MP4 single-frame reads should use sparse ``smart_load_content`` ranges."""
    video_path = tmp_path / "sample.mp4"
    _build_local_video(video_path, codec_name="mpeg4")
    boxes = _scan_mp4_top_level_boxes(str(video_path), video_path.stat().st_size)
    moov_box = next(box for box in boxes if box.type_name == "moov")

    load_spy = mocker.spy(video_module, "smart_load_content")

    with video_open(str(video_path), "r") as reader:
        frame = reader[3]

    assert frame.pts is not None
    assert load_spy.call_count > 0
    assert any(
        call.args[1] <= moov_box.offset
        and call.args[2] >= moov_box.offset + moov_box.size
        for call in load_spy.call_args_list
    )


def test_video_reader_slice_avoids_smart_load_content_after_open(
    tmp_path: Path,
    mocker,
):
    """Sequential slicing should continue to use the ``smart_open`` reader."""
    video_path = tmp_path / "sample.mp4"
    _build_local_video(video_path, codec_name="mpeg4")

    load_spy = mocker.spy(video_module, "smart_load_content")

    with video_open(str(video_path), "r") as reader:
        load_spy.reset_mock()
        frames = list(reader[1:4])

    assert [frame.pts for frame in frames]
    load_spy.assert_not_called()


def test_video_writer_roundtrip(tmp_path: Path):
    """VideoWriter should encode ndarray inputs and produce readable output."""
    video_path = tmp_path / "roundtrip.mkv"

    with video_open(
        str(video_path),
        "w",
        rate=5,
        width=16,
        height=16,
        container_format="matroska",
        codec_name="ffv1",
        pix_fmt="yuv420p",
    ) as writer:
        assert isinstance(writer, VideoWriter)
        for index in range(4):
            writer.append(np.full((16, 16, 3), index * 40, dtype=np.uint8))

    with video_open(str(video_path), "r") as reader:
        means = [int(frame.to_ndarray(format="rgb24").mean()) for frame in reader[:]]

    assert means == pytest.approx([0, 40, 80, 120], abs=1)


def test_build_hwaccel_rejects_unknown_device(mocker):
    """_build_hwaccel should reject unavailable device types."""
    mocker.patch("av.codec.hwaccel.hwdevices_available", return_value=["cuda"])

    with pytest.raises(ValueError, match="unsupported video hwaccel device type"):
        _build_hwaccel("vaapi")


def test_video_open_forwards_arguments_to_reader(mocker):
    """video_open should forward read arguments to VideoReader."""
    file_object = _TrackingBytesIO(b"")
    open_func = mocker.Mock(return_value=file_object)
    reader = mocker.Mock(spec=VideoReader)
    reader_cls = mocker.patch("megstore.video.VideoReader", return_value=reader)

    result = video_open(
        "sample.mp4",
        "r",
        stream_index=1,
        hwaccel="cuda",
        hwaccel_device="0",
        hwaccel_allow_software_fallback=True,
        hwaccel_options={"surfaces": "8"},
        open_func=open_func,
    )

    assert result is reader
    reader_cls.assert_called_once()
    _, kwargs = reader_cls.call_args
    assert kwargs == {
        "stream_index": 1,
        "container_options": None,
        "hwaccel": "cuda",
        "hwaccel_device": "0",
        "hwaccel_allow_software_fallback": True,
        "hwaccel_options": {"surfaces": "8"},
        "read_buffer_size": DEFAULT_VIDEO_READ_BUFFER_SIZE,
        "single_frame_cache_block_size": (
            video_module.DEFAULT_VIDEO_SINGLE_FRAME_CACHE_BLOCK_SIZE
        ),
        "close_fileobj_when_close": True,
    }


def test_video_reader_open_container_uses_hwaccel(mocker):
    """VideoReader should pass hwaccel settings to av.open."""
    fake_hwaccel = object()
    mocker.patch("megstore.video._build_hwaccel", return_value=fake_hwaccel)
    mocker.patch(
        "megstore.video.VideoReader._probe_stream_info",
        return_value=VideoStreamInfo(
            stream_index=0,
            frame_count=0,
            first_frame_pts=0,
            keyframe_timestamps=(),
            rate=None,
            time_base=None,
            codec_name=None,
            width=0,
            height=0,
            pix_fmt=None,
        ),
    )

    fake_container = mocker.Mock()
    fake_av = mocker.Mock()
    fake_av.open.return_value = fake_container
    mocker.patch("megstore.video._ensure_av", return_value=fake_av)

    with VideoReader(
        _TrackingBytesIO(b""),
        hwaccel="cuda",
        read_buffer_size=4096,
    ) as reader:
        container = reader._open_container(_TrackingBytesIO(b""))

    assert container is fake_container
    fake_av.open.assert_called_once_with(
        mocker.ANY,
        "r",
        buffer_size=4096,
        hwaccel=fake_hwaccel,
    )


def test_video_reader_uses_stream_start_time_without_decoding(mocker):
    """VideoReader should trust ``start_time`` metadata without decoding frames."""
    file_object = _TrackingBytesIO(b"")
    fake_container = mocker.Mock()
    fake_container.decode.side_effect = AssertionError("decode should not be called")

    codec_context = mocker.Mock()
    codec_context.name = "mpeg4"
    codec_context.width = 16
    codec_context.height = 16
    codec_context.pix_fmt = "yuv420p"

    fake_stream = mocker.Mock()
    fake_stream.index = 0
    fake_stream.frames = 4
    fake_stream.duration = None
    fake_stream.time_base = Fraction(1, 90000)
    fake_stream.start_time = 3600
    fake_stream.codec_context = codec_context

    mocker.patch("megstore.video.reopen", return_value=(file_object, False))
    mocker.patch(
        "megstore.video.VideoReader._open_container",
        return_value=fake_container,
    )
    mocker.patch("megstore.video._select_video_stream", return_value=fake_stream)
    mocker.patch("megstore.video._collect_index_entries", return_value=())
    mocker.patch("megstore.video._resolve_stream_rate", return_value=Fraction(5, 1))

    with VideoReader(file_object) as reader:
        assert reader._stream_info.first_frame_pts == 3600
        assert reader.average_rate == Fraction(5, 1)
        assert reader.time_base == Fraction(1, 90000)

    fake_container.decode.assert_not_called()


def test_video_reader_requires_constant_frame_rate_metadata(mocker):
    """VideoReader should reject streams without constant frame-rate metadata."""
    file_object = _TrackingBytesIO(b"")
    fake_container = mocker.Mock()

    codec_context = mocker.Mock()
    codec_context.name = "mpeg4"
    codec_context.width = 16
    codec_context.height = 16
    codec_context.pix_fmt = "yuv420p"

    fake_stream = mocker.Mock()
    fake_stream.index = 0
    fake_stream.frames = 4
    fake_stream.duration = None
    fake_stream.time_base = None
    fake_stream.start_time = 0
    fake_stream.codec_context = codec_context

    mocker.patch("megstore.video.reopen", return_value=(file_object, False))
    mocker.patch(
        "megstore.video.VideoReader._open_container",
        return_value=fake_container,
    )
    mocker.patch("megstore.video._select_video_stream", return_value=fake_stream)
    mocker.patch("megstore.video._collect_index_entries", return_value=())
    mocker.patch("megstore.video._resolve_stream_rate", return_value=None)

    with pytest.raises(
        ValueError, match="frame index access requires constant frame rate metadata"
    ):
        VideoReader(file_object)


def test_video_reader_rejects_non_uniform_frame_timestamps(mocker):
    """VideoReader should reject decoded timestamps that do not match CFR math."""
    mocker.patch(
        "megstore.video.VideoReader._probe_stream_info",
        return_value=VideoStreamInfo(
            stream_index=0,
            frame_count=4,
            first_frame_pts=0,
            keyframe_timestamps=(),
            rate=Fraction(5, 1),
            time_base=Fraction(1, 10),
            codec_name=None,
            width=0,
            height=0,
            pix_fmt=None,
        ),
    )

    with VideoReader(_TrackingBytesIO(b"")) as reader:
        with pytest.raises(
            ValueError, match="non-uniform frame timestamps are not supported"
        ):
            reader._frame_index_from_pts(3)


def test_video_writer_uses_keyword_arguments(mocker):
    """VideoWriter should initialize encoder settings from keyword arguments."""
    fake_container = mocker.Mock()
    fake_av = mocker.Mock()
    fake_av.open.return_value = fake_container
    mocker.patch("megstore.video._ensure_av", return_value=fake_av)

    with VideoWriter(
        _TrackingBytesIO(b""),
        rate=5,
        width=16,
        height=16,
        container_format="matroska",
        codec_name="ffv1",
        pix_fmt="yuv420p",
        metadata={"title": "sample"},
    ) as writer:
        assert writer._codec_name == "ffv1"
        assert writer._rate == 5
        assert writer._width == 16
        assert writer._height == 16
        assert writer._pix_fmt == "yuv420p"

    fake_av.open.assert_called_once_with(
        mocker.ANY,
        "w",
        format="matroska",
        container_options={},
    )
    fake_container.metadata.update.assert_called_once_with({"title": "sample"})


def test_video_open_uses_default_software_codec(tmp_path: Path):
    """video_open should default to the built-in software encoder."""
    video_path = tmp_path / "default-codec.mp4"

    with video_open(
        str(video_path),
        "w",
        rate=5,
        width=16,
        height=16,
        container_format="mp4",
    ) as writer:
        assert writer._codec_name == "libx264"


def test_video_open_write_requires_rate_width_height(tmp_path: Path):
    """video_open should require rate, width, and height in write mode."""
    video_path = tmp_path / "missing-size.mp4"

    with pytest.raises(ValueError, match="rate, width, and height are required"):
        video_open(str(video_path), "w", container_format="mp4")


@mock_s3
def test_video_s3_roundtrip(mocker):
    """Video read and write should work with S3/OSS-style paths via megfile."""
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="bucket")
    mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
    mocker.patch("megfile.s3_path.get_s3_client_with_cache", return_value=client)

    video_path = "s3://bucket/sample.mp4"

    with video_open(
        video_path,
        "w",
        rate=5,
        width=16,
        height=16,
        container_format="mp4",
        codec_name="mpeg4",
    ) as writer:
        for index in range(3):
            writer.append(np.full((16, 16, 3), index * 50, dtype=np.uint8))

    keys = sorted(
        content["Key"]
        for content in client.list_objects_v2(Bucket="bucket")["Contents"]
    )
    assert keys == ["sample.mp4"]

    with video_open(video_path, "r") as reader:
        assert len(reader) == 3
        assert [frame.pts for frame in reader[:]] == [frame.pts for frame in reader]

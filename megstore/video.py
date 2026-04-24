from __future__ import annotations

import os
from bisect import bisect_right
from dataclasses import dataclass
from fractions import Fraction
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Union,
    cast,
)

from megfile import smart_getsize, smart_load_content, smart_open

from megstore.interface import BaseReader, BaseWriter, OpenBinaryIO, reopen
from megstore.utils import smart_limited_seekable_open

if TYPE_CHECKING:
    import av

__all__ = [
    "VideoReader",
    "VideoWriter",
    "video_open",
]

DEFAULT_VIDEO_CODEC_NAME = "libx264"
DEFAULT_VIDEO_PIX_FMT = "yuv420p"
DEFAULT_VIDEO_FRAME_FORMAT = "rgb24"
DEFAULT_VIDEO_READ_BUFFER_SIZE = 256 * 2**10
DEFAULT_VIDEO_REMOTE_BLOCK_SIZE = 8 * 2**20
DEFAULT_VIDEO_REMOTE_BLOCK_FORWARD = 8
DEFAULT_VIDEO_REMOTE_BUFFER_SIZE = 64 * 2**20
DEFAULT_VIDEO_SINGLE_FRAME_CACHE_BLOCK_SIZE = 256 * 2**10


def _ensure_av():
    """Import PyAV lazily.

    :raises ImportError: If PyAV is not installed.
    :returns: Imported ``av`` module.
    """
    try:
        import av
    except ImportError as error:
        raise ImportError(
            "PyAV is required to use megstore.video, "
            "please install it with `pip install 'megstore[video]'`"
        ) from error
    return av


def _normalize_fraction(value: Any) -> Optional[Fraction]:
    """Convert a rational-like value to ``Fraction``.

    :param value: Source value.
    :returns: Fraction value or ``None``.
    """
    if value is None:
        return None
    return Fraction(value.numerator, value.denominator)


def _round_fraction(value: Fraction) -> int:
    """Round a fraction to the nearest integer.

    :param value: Fraction value.
    :returns: Rounded integer.
    """
    if value >= 0:
        return int(value + Fraction(1, 2))
    return -int((-value) + Fraction(1, 2))


def _make_share_cache_key(path: str, source_size: Optional[int]) -> str:
    """Create a stable share-cache key for remote video reads.

    :param path: Video path.
    :param source_size: Current file size.
    :returns: Share-cache key string.
    """
    return "megstore-video:%s:%s" % (path, source_size)


def _select_video_stream(container: Any, stream_index: Optional[int]) -> Any:
    """Pick a video stream from a PyAV container.

    :param container: Open PyAV input container.
    :param stream_index: Optional container stream index or video-stream ordinal.
    :raises ValueError: If no matching video stream exists.
    :returns: Selected video stream.
    """
    video_streams = list(container.streams.video)
    if not video_streams:
        raise ValueError("no video stream found in %r" % container)
    if stream_index is None:
        return video_streams[0]
    for stream in video_streams:
        if stream.index == stream_index:
            return stream
    if 0 <= stream_index < len(video_streams):
        return video_streams[stream_index]
    raise ValueError("video stream not found: %r" % stream_index)


def _normalize_frame(value: Any, frame_format: str):
    """Convert user input into a PyAV ``VideoFrame``.

    :param value: Input frame value.
    :param frame_format: Expected ndarray pixel format.
    :raises TypeError: If the input cannot be converted into a video frame.
    :returns: ``av.VideoFrame`` instance.
    """
    av = _ensure_av()
    if isinstance(value, av.VideoFrame):
        return value
    try:
        return av.VideoFrame.from_ndarray(value, format=frame_format)
    except Exception as error:
        raise TypeError(
            "unsupported video frame value: %s" % type(value).__qualname__
        ) from error


def _build_hwaccel(
    hwaccel: Optional[str],
    *,
    hwaccel_device: Optional[Union[str, int]] = None,
    hwaccel_allow_software_fallback: bool = False,
    hwaccel_options: Optional[Mapping[str, object]] = None,
) -> Optional[Any]:
    """Create a PyAV hardware-accelerated decoding configuration.

    :param hwaccel: Hardware acceleration device type such as ``"cuda"``.
    :param hwaccel_device: Optional device identifier passed to PyAV.
    :param hwaccel_allow_software_fallback: Whether software fallback is allowed.
    :param hwaccel_options: Optional device options passed to PyAV.
    :raises ValueError: If the requested hardware acceleration type is unavailable.
    :returns: ``av.codec.hwaccel.HWAccel`` instance or ``None``.
    """
    if hwaccel is None:
        return None

    _ensure_av()
    from av.codec.hwaccel import (  # pytype: disable=import-error
        HWAccel,
        hwdevices_available,
    )

    available_devices = hwdevices_available()
    if hwaccel not in available_devices:
        raise ValueError(
            "unsupported video hwaccel device type: %r, available: %r"
            % (hwaccel, available_devices)
        )

    return HWAccel(
        hwaccel,
        device=hwaccel_device,
        allow_software_fallback=hwaccel_allow_software_fallback,
        options=dict(hwaccel_options or {}),
    )


def _resolve_stream_rate(stream: Any) -> Optional[Fraction]:
    """Resolve the best available frame rate from a stream.

    :param stream: PyAV stream.
    :returns: Frame rate as ``Fraction`` or ``None``.
    """
    for attr_name in ("average_rate", "guessed_rate", "base_rate"):
        value = _normalize_fraction(getattr(stream, attr_name, None))
        if value:
            return value
    return None


@dataclass(frozen=True, slots=True)
class _VideoIndexEntry:
    """Container index entry with byte offset metadata."""

    timestamp: int
    pos: Optional[int]
    size: Optional[int]
    is_keyframe: bool


@dataclass(frozen=True, slots=True)
class _ByteRange:
    """Half-open byte range used by the sparse MP4 cache."""

    start: int
    stop: int


@dataclass(frozen=True, slots=True)
class _Mp4Box:
    """Top-level MP4 box metadata."""

    offset: int
    size: int
    type_name: str
    header_size: int


def _collect_index_entries(stream: Any) -> tuple[_VideoIndexEntry, ...]:
    """Collect container index entries with packet offsets.

    :param stream: PyAV stream.
    :returns: Sorted tuple of index entries.
    """
    return tuple(
        _VideoIndexEntry(
            timestamp=int(entry.timestamp),
            pos=None if getattr(entry, "pos", None) is None else int(entry.pos),
            size=None if getattr(entry, "size", None) is None else int(entry.size),
            is_keyframe=bool(getattr(entry, "is_keyframe", False)),
        )
        for entry in getattr(stream, "index_entries", ())
        if entry.timestamp is not None
    )


def _collect_keyframe_timestamps(
    entries: tuple[_VideoIndexEntry, ...],
) -> tuple[int, ...]:
    """Collect seekable timestamps from container index entries.

    :param entries: Collected container index entries.
    :returns: Sorted tuple of index timestamps.
    """
    keyframe_timestamps = tuple(
        entry.timestamp for entry in entries if entry.is_keyframe
    )
    if keyframe_timestamps:
        return keyframe_timestamps
    return tuple(entry.timestamp for entry in entries)


def _bisect_index_entry_timestamps(
    entries: tuple[_VideoIndexEntry, ...],
    target_timestamp: int,
) -> int:
    """Find the insertion position in sorted entry timestamps.

    :param entries: Sorted container index entries.
    :param target_timestamp: Target timestamp in stream time-base units.
    :returns: Right-side insertion position.
    """
    low = 0
    high = len(entries)
    while low < high:
        middle = (low + high) // 2
        if target_timestamp < entries[middle].timestamp:
            high = middle
        else:
            low = middle + 1
    return low


def _is_mp4_path(path: Optional[str]) -> bool:
    """Return whether a path should use the MP4 single-frame cache.

    :param path: Backing file path.
    :returns: ``True`` when the path looks like an MP4 file.
    """
    return bool(path and path.lower().endswith(".mp4"))


def _merge_byte_ranges(ranges: Iterable[_ByteRange]) -> tuple[_ByteRange, ...]:
    """Merge overlapping or adjacent byte ranges.

    :param ranges: Input byte ranges.
    :returns: Normalized byte ranges.
    """
    merged_ranges: list[_ByteRange] = []
    for byte_range in sorted(ranges, key=lambda value: (value.start, value.stop)):
        if byte_range.stop <= byte_range.start:
            continue
        if not merged_ranges:
            merged_ranges.append(byte_range)
            continue
        previous_range = merged_ranges[-1]
        if byte_range.start > previous_range.stop:
            merged_ranges.append(byte_range)
            continue
        merged_ranges[-1] = _ByteRange(
            start=previous_range.start,
            stop=max(previous_range.stop, byte_range.stop),
        )
    return tuple(merged_ranges)


def _read_mp4_box(path: str, offset: int, source_size: int) -> _Mp4Box:
    """Read one top-level MP4 box header.

    :param path: MP4 path.
    :param offset: Box offset in bytes.
    :param source_size: Full file size in bytes.
    :raises ValueError: If the MP4 box header is invalid.
    :returns: Parsed top-level MP4 box.
    """
    header = smart_load_content(path, offset, min(source_size, offset + 16))
    if len(header) < 8:
        raise ValueError("invalid mp4 box header at offset %d" % offset)

    box_size = int.from_bytes(header[:4], "big")
    type_name = header[4:8].decode("latin1")
    header_size = 8
    if box_size == 1:
        if len(header) < 16:
            raise ValueError("invalid 64-bit mp4 box header at offset %d" % offset)
        box_size = int.from_bytes(header[8:16], "big")
        header_size = 16
    elif box_size == 0:
        box_size = source_size - offset

    if box_size < header_size:
        raise ValueError("invalid mp4 box size at offset %d" % offset)

    return _Mp4Box(
        offset=offset,
        size=box_size,
        type_name=type_name,
        header_size=header_size,
    )


def _scan_mp4_top_level_boxes(path: str, source_size: int) -> tuple[_Mp4Box, ...]:
    """Scan top-level MP4 boxes with range reads.

    :param path: MP4 path.
    :param source_size: Full file size in bytes.
    :raises ValueError: If the MP4 structure is invalid.
    :returns: Parsed top-level MP4 boxes.
    """
    boxes = []
    offset = 0
    while offset < source_size:
        box = _read_mp4_box(path, offset, source_size)
        boxes.append(box)
        offset += box.size
    return tuple(boxes)


def _build_mp4_metadata_ranges(boxes: tuple[_Mp4Box, ...]) -> tuple[_ByteRange, ...]:
    """Build sparse-cache preload ranges from top-level MP4 boxes.

    :param boxes: Parsed top-level MP4 boxes.
    :returns: Merged byte ranges that should be cached up front.
    """
    full_box_types = frozenset(
        {
            "ftyp",
            "meta",
            "moof",
            "moov",
            "mfra",
            "prft",
            "sidx",
            "ssix",
            "styp",
        }
    )
    ranges = []
    for box in boxes:
        if box.type_name in full_box_types:
            ranges.append(_ByteRange(box.offset, box.offset + box.size))
            continue
        ranges.append(_ByteRange(box.offset, box.offset + box.header_size))
    return _merge_byte_ranges(ranges)


class _SparseContentReader:
    """A seekable sparse reader backed by ``smart_load_content``."""

    def __init__(
        self,
        path: str,
        *,
        size: int,
        block_size: int,
        preload_ranges: Iterable[_ByteRange] = (),
    ):
        """Initialize the sparse reader.

        :param path: Backing file path.
        :param size: Full source size.
        :param block_size: On-demand cache block size.
        :param preload_ranges: Byte ranges to cache eagerly.
        """
        self._path = path
        self._size = size
        self._offset = 0
        self._closed = False
        self._block_size = max(1, block_size)
        self._chunks: list[tuple[int, bytes]] = []
        self.name = path
        self.mode = "rb"

        for byte_range in _merge_byte_ranges(preload_ranges):
            self._load_range(byte_range.start, byte_range.stop)

    @property
    def closed(self) -> bool:
        """Return whether this wrapper is closed.

        :returns: ``True`` when the wrapper is closed.
        """
        return self._closed

    def _load_range(self, start: int, stop: int):
        """Load one byte range into the sparse cache.

        :param start: Range start offset.
        :param stop: Range stop offset.
        """
        start = max(0, start)
        stop = min(self._size, stop)
        if stop <= start:
            return
        data = smart_load_content(self._path, start, stop)
        if not data:
            return
        self._chunks.append((start, data))
        self._chunks.sort(key=lambda item: item[0])

    def _find_chunk(self, offset: int) -> Optional[tuple[int, bytes]]:
        """Find a cached chunk that covers an offset.

        :param offset: Target offset.
        :returns: Cached chunk when found.
        """
        for chunk_start, chunk_data in reversed(self._chunks):
            chunk_stop = chunk_start + len(chunk_data)
            if chunk_start <= offset < chunk_stop:
                return chunk_start, chunk_data
        return None

    def _ensure_cached(self, offset: int, stop: int):
        """Ensure an offset is covered by at least one cached chunk.

        :param offset: First required offset.
        :param stop: Preferred stop offset for the read.
        """
        if self._find_chunk(offset) is not None:
            return
        aligned_start = offset - (offset % self._block_size)
        aligned_stop = min(self._size, max(stop, aligned_start + self._block_size))
        self._load_range(aligned_start, aligned_stop)

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the current logical offset.

        :param size: Maximum bytes to read.
        :returns: Read bytes.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        remaining = self._size - self._offset
        if size is None or size < 0:
            size = remaining
        size = max(0, min(size, remaining))
        if size == 0:
            return b""

        chunks = []
        offset = self._offset
        unread_size = size
        requested_stop = min(self._size, offset + size)

        while unread_size > 0:
            cached_chunk = self._find_chunk(offset)
            if cached_chunk is None:
                self._ensure_cached(offset, requested_stop)
                cached_chunk = self._find_chunk(offset)
                if cached_chunk is None:
                    break
            chunk_start, chunk_data = cached_chunk
            chunk_offset = offset - chunk_start
            consumed = min(unread_size, len(chunk_data) - chunk_offset)
            if consumed <= 0:
                break
            chunks.append(chunk_data[chunk_offset : chunk_offset + consumed])
            offset += consumed
            unread_size -= consumed

        self._offset = offset
        return b"".join(chunks)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Move the logical file offset.

        :param offset: Seek offset.
        :param whence: Seek mode.
        :returns: New offset.
        """
        if whence == 0:
            new_offset = offset
        elif whence == 1:
            new_offset = self._offset + offset
        elif whence == 2:
            new_offset = self._size + offset
        else:
            raise ValueError("invalid whence: %r" % whence)
        self._offset = max(0, new_offset)
        return self._offset

    def tell(self) -> int:
        """Return the current logical offset.

        :returns: Current offset.
        """
        return self._offset

    def readable(self) -> bool:
        """Return whether the wrapper is readable.

        :returns: Always ``True``.
        """
        return True

    def writable(self) -> bool:
        """Return whether the wrapper is writable.

        :returns: Always ``False``.
        """
        return False

    def seekable(self) -> bool:
        """Return whether the wrapper is seekable.

        :returns: Always ``True``.
        """
        return True

    def close(self):
        """Close the wrapper."""
        self._closed = True


@dataclass(frozen=True, slots=True)
class VideoStreamInfo:
    """Cached video stream metadata used for random access."""

    stream_index: int
    frame_count: int
    first_frame_pts: int
    keyframe_timestamps: tuple[int, ...]
    rate: Optional[Fraction]
    time_base: Optional[Fraction]
    codec_name: Optional[str]
    width: int
    height: int
    pix_fmt: Optional[str]
    index_entries: tuple[_VideoIndexEntry, ...] = ()


class VideoReader(BaseReader["av.VideoFrame"]):  # pytype: disable=not-indexable
    """Random-access video reader backed by PyAV built-in stream indexes."""

    def __init__(
        self,
        file_object: BinaryIO,
        *,
        stream_index: Optional[int] = None,
        container_options: Optional[Mapping[str, str]] = None,
        hwaccel: Optional[str] = None,
        hwaccel_device: Optional[Union[str, int]] = None,
        hwaccel_allow_software_fallback: bool = False,
        hwaccel_options: Optional[Mapping[str, object]] = None,
        read_buffer_size: int = DEFAULT_VIDEO_READ_BUFFER_SIZE,
        single_frame_cache_block_size: int = (
            DEFAULT_VIDEO_SINGLE_FRAME_CACHE_BLOCK_SIZE
        ),
        close_fileobj_when_close: bool = False,
    ):
        """Initialize a random-access video reader.

        :param file_object: Open binary video stream.
        :param stream_index: Optional container stream index or video-stream ordinal.
        :param container_options: Optional container-level options passed to PyAV.
        :param hwaccel: Optional hardware acceleration device type.
        :param hwaccel_device: Optional device identifier passed to PyAV.
        :param hwaccel_allow_software_fallback: Whether software fallback is allowed.
        :param hwaccel_options: Optional hardware acceleration options.
        :param read_buffer_size: Python-side input buffer size.
        :param single_frame_cache_block_size: Sparse cache block size used for
            MP4 single-frame random access.
        :param close_fileobj_when_close: Whether to close ``file_object`` on close.
        """
        super().__init__(file_object, close_fileobj_when_close=close_fileobj_when_close)
        self._requested_stream_index = stream_index
        self._container_options = dict(container_options or {})
        self._hwaccel = _build_hwaccel(
            hwaccel,
            hwaccel_device=hwaccel_device,
            hwaccel_allow_software_fallback=hwaccel_allow_software_fallback,
            hwaccel_options=hwaccel_options,
        )
        self._buffer_size = read_buffer_size
        self._single_frame_cache_block_size = single_frame_cache_block_size
        self._source_path = getattr(self._raw_file_object, "name", None)
        self._raw_file_object.seek(0, os.SEEK_END)
        self._source_size = self._raw_file_object.tell()
        self._raw_file_object.seek(0)

        self._mp4_top_level_boxes: Optional[tuple[_Mp4Box, ...]] = None
        self._mp4_metadata_ranges: Optional[tuple[_ByteRange, ...]] = None
        self._stream_info = self._probe_stream_info()

    @property
    def stream_index(self) -> int:
        """Return the selected stream index.

        :returns: Container stream index.
        """
        return self._stream_info.stream_index

    @property
    def width(self) -> int:
        """Return the output frame width.

        :returns: Frame width in pixels.
        """
        return self._stream_info.width

    @property
    def height(self) -> int:
        """Return the output frame height.

        :returns: Frame height in pixels.
        """
        return self._stream_info.height

    @property
    def codec_name(self) -> Optional[str]:
        """Return the selected stream codec name.

        :returns: Codec name or ``None``.
        """
        return self._stream_info.codec_name

    @property
    def average_rate(self) -> Optional[Fraction]:
        """Return the average frame rate reported by the container.

        :returns: Average frame rate.
        """
        return self._stream_info.rate

    @property
    def time_base(self) -> Optional[Fraction]:
        """Return the stream time base.

        :returns: Time base as ``Fraction``.
        """
        return self._stream_info.time_base

    @property
    def _container(self):
        """Get or create the current thread-local PyAV container."""
        return self._local("container", self._create_container)

    @property
    def _video_stream(self):
        """Get or create the current thread-local PyAV video stream."""
        return self._local("video_stream", self._create_video_stream)

    def _open_container(self, file_object: BinaryIO):
        """Open a PyAV input container on a file object.

        :param file_object: Binary file object.
        :returns: Opened PyAV input container.
        """
        av = _ensure_av()
        open_kwargs = {
            "buffer_size": self._buffer_size,
        }
        if self._container_options:
            open_kwargs["container_options"] = self._container_options
        if self._hwaccel is not None:
            open_kwargs["hwaccel"] = self._hwaccel
        return av.open(file_object, "r", **open_kwargs)

    def _create_container(self):
        """Create a thread-local input container.

        :returns: Opened PyAV input container.
        """
        return self._open_container(self._file_object)

    def _create_video_stream(self):
        """Select the current video stream from the thread-local container.

        :returns: Selected PyAV stream.
        """
        return _select_video_stream(self._container, self._stream_info.stream_index)

    def _resolve_frame_count(self, container: Any, stream: Any) -> Optional[int]:
        """Resolve frame count from stream/container metadata.

        :param container: PyAV container.
        :param stream: PyAV video stream.
        :returns: Frame count or ``None`` when metadata is insufficient.
        """
        if getattr(stream, "frames", 0) > 0:
            return int(stream.frames)

        rate = _resolve_stream_rate(stream)
        if rate and stream.duration is not None and stream.time_base is not None:
            duration = Fraction(stream.duration) * Fraction(stream.time_base)
            return max(0, _round_fraction(duration * rate))

        av = _ensure_av()
        if rate and container.duration is not None:
            duration = Fraction(container.duration, av.time_base)
            return max(0, _round_fraction(duration * rate))

        return None

    def _probe_stream_info(self) -> VideoStreamInfo:
        """Probe stream metadata without creating an external sidecar index.

        :raises ValueError: If constant frame-rate metadata is unavailable.
        :returns: Stream metadata used for random access.
        """
        file_object, _ = reopen(self._raw_file_object)
        file_object = cast(BinaryIO, file_object)
        container = self._open_container(file_object)  # pytype: disable=wrong-arg-types
        try:
            stream = _select_video_stream(container, self._requested_stream_index)
            index_entries = _collect_index_entries(stream)
            rate = _resolve_stream_rate(stream)
            time_base = _normalize_fraction(getattr(stream, "time_base", None))
            first_frame_pts = int(getattr(stream, "start_time", 0) or 0)
            frame_count = self._resolve_frame_count(container, stream)
            if rate is None or time_base is None:
                raise ValueError(
                    "frame index access requires constant frame rate metadata"
                )
            if frame_count is None:
                raise ValueError("frame index access requires frame count metadata")

            return VideoStreamInfo(
                stream_index=stream.index,
                frame_count=frame_count,
                first_frame_pts=first_frame_pts,
                keyframe_timestamps=_collect_keyframe_timestamps(index_entries),
                rate=rate,
                time_base=time_base,
                codec_name=getattr(stream.codec_context, "name", None),
                width=int(getattr(stream.codec_context, "width", 0)),
                height=int(getattr(stream.codec_context, "height", 0)),
                pix_fmt=getattr(stream.codec_context, "pix_fmt", None),
                index_entries=index_entries,
            )
        finally:
            container.close()
            if file_object is not self._raw_file_object:
                file_object.close()

    def _supports_mp4_single_frame_cache(self) -> bool:
        """Return whether MP4 sparse single-frame access is available.

        :returns: ``True`` when the current source supports sparse MP4 reads.
        """
        return (
            self._source_path is not None
            and self._source_size is not None
            and _is_mp4_path(self._source_path)
            and bool(self._stream_info.index_entries)
        )

    def _mp4_boxes(self) -> tuple[_Mp4Box, ...]:
        """Return cached top-level MP4 boxes.

        :returns: Parsed top-level MP4 boxes.
        """
        if self._mp4_top_level_boxes is None:
            if self._source_path is None or self._source_size is None:
                return ()
            self._mp4_top_level_boxes = _scan_mp4_top_level_boxes(
                self._source_path,
                self._source_size,
            )
        return self._mp4_top_level_boxes

    def _mp4_metadata_preload_ranges(self) -> tuple[_ByteRange, ...]:
        """Return sparse-cache preload ranges for MP4 metadata.

        :returns: MP4 metadata ranges.
        """
        if self._mp4_metadata_ranges is None:
            self._mp4_metadata_ranges = _build_mp4_metadata_ranges(self._mp4_boxes())
        return self._mp4_metadata_ranges

    def _mp4_packet_range(self, index: int) -> Optional[_ByteRange]:
        """Compute the packet byte range needed for one frame decode.

        :param index: Target frame index.
        :returns: Packet byte range or ``None`` when unavailable.
        """
        entries = self._stream_info.index_entries
        if not entries:
            return None

        target_pts = self._target_pts(index)
        target_position = _bisect_index_entry_timestamps(entries, target_pts) - 1
        if target_position < 0:
            target_position = 0

        anchor_position = target_position
        while anchor_position > 0:
            anchor_entry = entries[anchor_position]
            if (
                anchor_entry.is_keyframe
                and anchor_entry.pos is not None
                and anchor_entry.size is not None
            ):
                break
            anchor_position -= 1

        anchor_entry = entries[anchor_position]
        if anchor_entry.pos is None or anchor_entry.size is None:
            return None

        stop_position = min(len(entries) - 1, target_position + 1)
        while stop_position > target_position:
            stop_entry = entries[stop_position]
            if stop_entry.pos is not None and stop_entry.size is not None:
                break
            stop_position -= 1
        stop_entry = entries[stop_position]
        if stop_entry.pos is None or stop_entry.size is None:
            return None

        return _ByteRange(
            start=anchor_entry.pos,
            stop=stop_entry.pos + stop_entry.size,
        )

    def _get_mp4_frame(self, index: int):
        """Read one MP4 frame via sparse ``smart_load_content`` ranges.

        :param index: Target frame index.
        :returns: Decoded ``av.VideoFrame`` or ``None`` when unavailable.
        """
        if not self._supports_mp4_single_frame_cache():
            return None

        try:
            metadata_ranges = self._mp4_metadata_preload_ranges()
            packet_range = self._mp4_packet_range(index)
            if packet_range is None:
                return None

            file_object = _SparseContentReader(
                self._source_path,
                size=self._source_size,
                block_size=self._single_frame_cache_block_size,  # type: ignore
                preload_ranges=metadata_ranges + (packet_range,),
            )
            container = self._open_container(  # pytype: disable=wrong-arg-types
                cast(BinaryIO, file_object)
            )
            try:
                stream = _select_video_stream(container, self._stream_info.stream_index)
                target_pts = self._target_pts(index)
                anchor_pts = self._anchor_pts(target_pts)
                container.seek(
                    anchor_pts, stream=stream, backward=True, any_frame=False
                )

                for frame in container.decode(stream):
                    if frame.pts is None:
                        continue
                    frame_index = self._frame_index_from_pts(int(frame.pts))
                    if frame_index < index:
                        continue
                    if frame_index == index:
                        return frame
                    if frame_index > index:
                        return None
            finally:
                container.close()
                file_object.close()
        except Exception:
            return None
        return None

    def _pts_per_frame(self) -> Fraction:
        """Return the frame duration in stream PTS units.

        :raises ValueError: If stream metadata is insufficient.
        :returns: PTS delta per frame.
        """
        if self._stream_info.rate is None or self._stream_info.time_base is None:
            raise ValueError(
                "frame index access requires stream rate and time_base metadata"
            )
        return Fraction(1, 1) / (self._stream_info.rate * self._stream_info.time_base)

    def _target_pts(self, index: int) -> int:
        """Resolve the expected PTS for a frame index.

        :param index: Frame index.
        :returns: Target PTS.
        """
        return self._stream_info.first_frame_pts + _round_fraction(
            Fraction(index) * self._pts_per_frame()
        )

    def _anchor_pts(self, target_pts: int) -> int:
        """Choose a seek anchor from the built-in container index.

        :param target_pts: Target frame PTS.
        :returns: Seek anchor PTS.
        """
        timestamps = self._stream_info.keyframe_timestamps
        if not timestamps:
            return self._stream_info.first_frame_pts
        position = bisect_right(timestamps, target_pts) - 1
        if position < 0:
            return timestamps[0]
        return timestamps[position]

    def _frame_index_from_pts(self, pts: int) -> int:
        """Map a decoded PTS value back to a frame index.

        :param pts: Frame PTS.
        :raises ValueError: If decoded timestamps do not follow the expected grid.
        :returns: Frame index.
        """
        frame_index = max(
            0,
            _round_fraction(
                Fraction(pts - self._stream_info.first_frame_pts)
                / self._pts_per_frame()
            ),
        )
        if pts != self._target_pts(frame_index):
            raise ValueError("non-uniform frame timestamps are not supported")
        return frame_index

    def _iter_from_index(
        self, start_index: int, stop_index: int
    ) -> Iterator["av.VideoFrame"]:
        """Decode frames in ``[start_index, stop_index)`` using one seek.

        :param start_index: Start frame index.
        :param stop_index: Stop frame index.
        :raises ValueError: If the container data is shorter than expected.
        :returns: Iterator of decoded video frames.
        """
        if start_index >= stop_index:
            return

        start_pts = self._target_pts(start_index)
        stop_count = stop_index - start_index
        anchor_pts = self._anchor_pts(start_pts)
        container = self._container
        stream = self._video_stream

        container.seek(anchor_pts, stream=stream, backward=True, any_frame=False)
        emitted = 0

        for frame in container.decode(stream):
            if frame.pts is None:
                continue
            frame_index = self._frame_index_from_pts(int(frame.pts))
            if frame_index < start_index:
                continue
            if frame_index >= stop_index:
                return
            yield frame
            emitted += 1
            if emitted >= stop_count:
                return

        raise ValueError(
            "out of data: %r, index: %d ~ %d" % (self.name, start_index, stop_index)
        )

    def get(self, index: int):  # pytype: disable=invalid-annotation
        """Read a frame by exact frame index.

        :param index: Target frame index.
        :returns: Decoded ``av.VideoFrame``.
        """
        index = range(self.count())[index]
        frame = self._get_mp4_frame(index)
        if frame is not None:
            return frame
        for frame in self._iter_from_index(index, index + 1):
            return frame
        raise ValueError("failed to decode frame: %r, index: %d" % (self.name, index))

    def _batch_get(self, index_slice: slice) -> Iterator["av.VideoFrame"]:
        """Read a contiguous frame slice using a single seek.

        :param index_slice: Slice object.
        :returns: Iterator of decoded frames.
        """
        start_index, stop_index, step = index_slice.indices(self.count())
        if step != 1 or start_index >= stop_index:
            yield from super()._batch_get(index_slice)
            return
        yield from self._iter_from_index(start_index, stop_index)

    def count(self) -> int:
        """Return the total frame count.

        :returns: Number of frames.
        """
        return self._stream_info.frame_count

    def _close_container(self):
        """Close the current thread-local PyAV container if it exists."""
        container = self._local.get("container")
        if container is not None:
            container.close()
            del self._local["container"]
        if self._local.get("video_stream") is not None:
            del self._local["video_stream"]

    def _close(self):
        """Close the reader and any open PyAV resources."""
        self._close_container()
        super()._close()

    def __del__(self):
        """Release PyAV resources during garbage collection."""
        self._close_container()
        super().__del__()


class VideoWriter(BaseWriter[Any]):
    """Sequential video writer backed by PyAV."""

    def __init__(
        self,
        file_object: BinaryIO,
        *,
        rate: Union[int, Fraction],
        width: int,
        height: int,
        container_format: Optional[str] = None,
        codec_name: str = DEFAULT_VIDEO_CODEC_NAME,
        pix_fmt: str = DEFAULT_VIDEO_PIX_FMT,
        frame_format: str = DEFAULT_VIDEO_FRAME_FORMAT,
        container_options: Optional[Mapping[str, str]] = None,
        stream_options: Optional[Mapping[str, str]] = None,
        metadata: Optional[Mapping[str, str]] = None,
        close_fileobj_when_close: bool = False,
    ):
        """Initialize a sequential video writer.

        :param file_object: Open binary output stream.
        :param rate: Output frame rate.
        :param width: Output frame width.
        :param height: Output frame height.
        :param container_format: Optional container format name.
        :param codec_name: Encoder codec name.
        :param pix_fmt: Pixel format requested from the encoder stream.
        :param frame_format: Pixel format expected from ndarray-like inputs.
        :param container_options: Optional container-level options passed to PyAV.
        :param stream_options: Optional stream-level encoder options.
        :param metadata: Optional container metadata written to the output file.
        :param close_fileobj_when_close: Whether to close ``file_object`` on close.
        """
        super().__init__(
            file_object,
            append_mode=False,
            close_fileobj_when_close=close_fileobj_when_close,
        )
        av = _ensure_av()
        self._container = av.open(
            file_object,
            "w",
            format=container_format,
            container_options=dict(container_options or {}),
        )
        if metadata:
            self._container.metadata.update(dict(metadata))

        self._codec_name = codec_name
        self._rate = rate
        self._width = width
        self._height = height
        self._pix_fmt = pix_fmt
        self._frame_format = frame_format
        self._stream_options = dict(stream_options or {})
        self._stream = None
        self._count = 0

    def _ensure_stream(self, frame: "av.VideoFrame"):
        """Create the encoder stream when the first frame arrives.

        :param frame: First video frame.
        :returns: Configured PyAV stream.
        """
        if self._stream is not None:
            return self._stream

        stream = self._container.add_stream(self._codec_name, rate=self._rate)
        stream.width = self._width
        stream.height = self._height
        stream.pix_fmt = self._pix_fmt
        if self._stream_options:
            stream.options = self._stream_options

        self._stream = stream
        return stream

    def append(self, value: Any):
        """Encode and append a single frame.

        :param value: ``av.VideoFrame`` or an ndarray-like object.
        """
        frame = _normalize_frame(value, self._frame_format)
        stream = self._ensure_stream(frame)
        frame = frame.reformat(
            width=stream.width,
            height=stream.height,
            format=stream.pix_fmt,
        )
        for packet in stream.encode(frame):
            self._container.mux(packet)
        self._count += 1

    def count(self) -> int:
        """Return the number of frames appended so far.

        :returns: Number of frames.
        """
        return self._count

    def commit(self):
        """Flush Python-side buffers without finalizing the video stream."""
        if hasattr(self._file_object, "flush"):
            self._file_object.flush()

    def _close(self):
        """Finalize the container and close the underlying file object."""
        if self._stream is not None:
            for packet in self._stream.encode(None):
                self._container.mux(packet)
        self._container.close()

        if hasattr(self._file_object, "flush"):
            self._file_object.flush()
        if self._close_fileobj_when_close:
            self._file_object.close()


def _default_video_read_open(
    path: str,
    mode: str,
    *,
    share_cache_key: str,
    remote_block_size: int,
    remote_block_forward: int,
    remote_buffer_size: int,
) -> BinaryIO:
    """Open a video file for sequential reading with ``smart_open``.

    :param path: Video path.
    :param mode: Open mode, expected ``"rb"``.
    :param share_cache_key: Shared megfile cache key.
    :param remote_block_size: Remote block size.
    :param remote_block_forward: Remote block prefetch size.
    :param remote_buffer_size: Remote in-memory cache size.
    :returns: Binary file object.
    """
    return cast(
        BinaryIO,
        smart_open(
            path,
            mode,
            share_cache_key=share_cache_key,
            block_size=remote_block_size,
            block_forward=remote_block_forward,
            max_buffer_size=remote_buffer_size,
        ),
    )


def video_open(
    path: str,
    mode: str = "r",
    *,
    rate: Optional[Union[int, Fraction]] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    stream_index: Optional[int] = None,
    container_format: Optional[str] = None,
    codec_name: str = DEFAULT_VIDEO_CODEC_NAME,
    pix_fmt: str = DEFAULT_VIDEO_PIX_FMT,
    frame_format: str = DEFAULT_VIDEO_FRAME_FORMAT,
    container_options: Optional[Mapping[str, str]] = None,
    stream_options: Optional[Mapping[str, str]] = None,
    metadata: Optional[Mapping[str, str]] = None,
    hwaccel: Optional[str] = None,
    hwaccel_device: Optional[Union[str, int]] = None,
    hwaccel_allow_software_fallback: bool = False,
    hwaccel_options: Optional[Mapping[str, object]] = None,
    read_buffer_size: int = DEFAULT_VIDEO_READ_BUFFER_SIZE,
    share_cache_key: Optional[str] = None,
    remote_block_size: int = DEFAULT_VIDEO_REMOTE_BLOCK_SIZE,
    remote_block_forward: int = DEFAULT_VIDEO_REMOTE_BLOCK_FORWARD,
    remote_buffer_size: int = DEFAULT_VIDEO_REMOTE_BUFFER_SIZE,
    single_frame_cache_block_size: int = (DEFAULT_VIDEO_SINGLE_FRAME_CACHE_BLOCK_SIZE),
    open_func: Optional[OpenBinaryIO] = None,
    close_fileobj_when_close: bool = True,
) -> Union[VideoReader, VideoWriter]:
    """Open a video file for random read or sequential write.

    :param path: Video path. Local paths and OSS/S3-style URLs are both supported.
    :param mode: Supported modes are ``"r"`` and ``"w"``.
    :param rate: Required output frame rate when ``mode`` is ``"w"``.
    :param width: Required output frame width when ``mode`` is ``"w"``.
    :param height: Required output frame height when ``mode`` is ``"w"``.
    :param stream_index: Optional container stream index or video-stream ordinal.
    :param container_format: Optional container format name used in write mode.
    :param codec_name: Encoder codec name used in write mode.
    :param pix_fmt: Pixel format requested from the encoder output stream.
    :param frame_format: Pixel format expected from ndarray-like frame inputs.
    :param container_options: Optional container-level options passed to PyAV.
    :param stream_options: Optional stream-level encoder options used in write mode.
    :param metadata: Optional container metadata written in write mode.
    :param hwaccel: Optional hardware acceleration device type used in read mode.
    :param hwaccel_device: Optional decoder device identifier used in read mode.
    :param hwaccel_allow_software_fallback: Whether decoder software fallback is
        allowed in read mode.
    :param hwaccel_options: Optional hardware acceleration options used in read
        mode.
    :param read_buffer_size: Python-side input buffer size used in read mode.
    :param share_cache_key: Optional shared megfile cache key used in read mode.
    :param remote_block_size: Remote read block size used in read mode.
    :param remote_block_forward: Remote read lookahead window used in read mode.
    :param remote_buffer_size: Maximum in-memory remote read buffer size used in
        read mode.
    :param single_frame_cache_block_size: Sparse cache block size used for MP4
        single-frame random access in read mode.
    :param open_func: Optional binary open function.
    :param close_fileobj_when_close: Whether to close the opened file object.
    :raises ValueError: If ``mode`` is unsupported.
    :returns: ``VideoReader`` or ``VideoWriter`` instance.
    """
    if mode not in ("r", "w"):
        raise ValueError("unacceptable mode: %r" % mode)

    if mode == "r":
        if rate is not None or width is not None or height is not None:
            raise ValueError("rate, width, and height are only supported in write mode")
        source_size = None
        if open_func is None:
            source_size = smart_getsize(path)
        else:
            try:
                source_size = smart_getsize(path)
            except Exception:
                source_size = None
        if open_func is None:
            effective_share_cache_key = share_cache_key
            if effective_share_cache_key is None:
                effective_share_cache_key = _make_share_cache_key(path, source_size)
            open_func = partial(
                _default_video_read_open,
                share_cache_key=effective_share_cache_key,
                remote_block_size=remote_block_size,
                remote_block_forward=remote_block_forward,
                remote_buffer_size=remote_buffer_size,
            )
        file_object = open_func(path, "rb")
        return VideoReader(
            file_object,
            stream_index=stream_index,
            container_options=container_options,
            hwaccel=hwaccel,
            hwaccel_device=hwaccel_device,
            hwaccel_allow_software_fallback=hwaccel_allow_software_fallback,
            hwaccel_options=hwaccel_options,
            read_buffer_size=read_buffer_size,
            single_frame_cache_block_size=single_frame_cache_block_size,
            close_fileobj_when_close=close_fileobj_when_close,
        )

    if open_func is None:
        open_func = smart_limited_seekable_open
    if rate is None or width is None or height is None:
        raise ValueError("rate, width, and height are required in write mode")
    file_object = open_func(path, "wb")
    return VideoWriter(
        file_object,
        rate=rate,
        width=width,
        height=height,
        container_format=container_format,
        codec_name=codec_name,
        pix_fmt=pix_fmt,
        frame_format=frame_format,
        container_options=container_options,
        stream_options=stream_options,
        metadata=metadata,
        close_fileobj_when_close=close_fileobj_when_close,
    )

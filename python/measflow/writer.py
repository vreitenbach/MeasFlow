"""Writer for the .meas binary format."""

from __future__ import annotations

import struct
import time
import uuid
from typing import Any, Union

import numpy as np

from measflow.types import MeasDataType, MeasTimestamp, MeasValue, _TYPE_NUMPY
from measflow._codec import (
    FileHeader,
    SegmentHeader,
    SegmentType,
    GroupDef,
    ChannelDef,
    encode_metadata,
    CHUNK_HEADER_FMT,
    FILE_HEADER_SIZE,
    SEGMENT_HEADER_SIZE,
)


class ChannelWriter:
    """Buffers samples for a single channel."""

    def __init__(self, name: str, dtype: MeasDataType) -> None:
        self.name = name
        self.data_type = dtype
        self.properties: dict[str, Any] = {}
        self._global_index: int = 0  # assigned when metadata is written
        self._samples: list = []

    def write(self, value: Any) -> None:
        """Append a single sample."""
        self._samples.append(value)

    def write_bulk(self, values: Any) -> None:
        """Append an array or iterable of samples."""
        self._samples.extend(values)

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    def _to_bytes(self) -> bytes:
        if not self._samples:
            return b""
        dt = self.data_type
        if dt == MeasDataType.Timestamp:
            ns = [
                v.nanoseconds if isinstance(v, MeasTimestamp) else int(v)
                for v in self._samples
            ]
            return np.array(ns, dtype="<i8").tobytes()
        if dt in _TYPE_NUMPY:
            return np.array(self._samples, dtype=_TYPE_NUMPY[dt]).tobytes()
        if dt == MeasDataType.Binary:
            # §7: each sample is [int32: frameByteLength][bytes: data]
            parts = []
            for v in self._samples:
                b = bytes(v)
                parts.append(struct.pack("<i", len(b)))
                parts.append(b)
            return b"".join(parts)
        if dt == MeasDataType.Utf8String:
            # §7: each sample is [int32: byteLength][UTF-8 bytes]
            parts = []
            for v in self._samples:
                encoded = v.encode("utf-8") if isinstance(v, str) else bytes(v)
                parts.append(struct.pack("<i", len(encoded)))
                parts.append(encoded)
            return b"".join(parts)
        raise ValueError(f"Cannot serialize channel type {dt!r}")

    def _to_channel_def(self) -> ChannelDef:
        props = {
            k: (v if isinstance(v, MeasValue) else MeasValue.from_python(v))
            for k, v in self.properties.items()
        }
        return ChannelDef(self.name, self.data_type, props)


class GroupWriter:
    """Collects channels for a single group."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.properties: dict[str, Any] = {}
        self._channels: list[ChannelWriter] = []

    def add_channel(
        self, name: str, dtype: MeasDataType = MeasDataType.Float64
    ) -> ChannelWriter:
        """Add a typed channel to this group."""
        ch = ChannelWriter(name, dtype)
        self._channels.append(ch)
        return ch

    def _to_group_def(self) -> GroupDef:
        props = {
            k: (v if isinstance(v, MeasValue) else MeasValue.from_python(v))
            for k, v in self.properties.items()
        }
        return GroupDef(self.name, props, [ch._to_channel_def() for ch in self._channels])


class MeasWriter:
    """Streaming writer for .meas files (§12.1).

    Supports incremental flush: each call to flush() writes a new Data segment.
    Use as a context manager or call close() explicitly.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._groups: list[GroupWriter] = []
        self._segment_count = 0
        self._metadata_written = False
        self._created_ns = int(time.time() * 1_000_000_000)
        self._file_id = uuid.uuid4().bytes
        # Open file immediately and write placeholder header
        self._file = open(path, "wb")
        self._file.write(b"\x00" * FILE_HEADER_SIZE)

    def add_group(self, name: str) -> GroupWriter:
        """Add a measurement group. Must be called before any data is written."""
        if self._metadata_written:
            raise RuntimeError("Cannot add groups after data has been written.")
        g = GroupWriter(name)
        self._groups.append(g)
        return g

    def flush(self) -> None:
        """Flush all buffered samples to disk as a new Data segment (§12.1)."""
        self._ensure_metadata()
        pending = [
            (ch._global_index, ch)
            for g in self._groups
            for ch in g._channels
            if ch._samples
        ]
        if not pending:
            return
        self._write_data_segment(pending)
        for _, ch in pending:
            ch._samples = []
        self._file.flush()

    def close(self) -> None:
        """Flush remaining data and finalise the file header."""
        if self._file.closed:
            return
        try:
            self.flush()
            # Patch SegmentCount in file header
            hdr = FileHeader(
                created_at_nanos=self._created_ns,
                segment_count=self._segment_count,
                file_id=self._file_id,
            )
            self._file.seek(0)
            self._file.write(hdr.to_bytes())
        finally:
            self._file.close()

    def __enter__(self) -> "MeasWriter":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _ensure_metadata(self) -> None:
        if self._metadata_written:
            return
        self._metadata_written = True
        # Assign global channel indices
        global_idx = 0
        for g in self._groups:
            for ch in g._channels:
                ch._global_index = global_idx
                global_idx += 1
        # Write actual file header (replace placeholder)
        hdr = FileHeader(
            created_at_nanos=self._created_ns,
            segment_count=0,
            file_id=self._file_id,
        )
        self._file.seek(0)
        self._file.write(hdr.to_bytes())
        self._file.seek(0, 2)  # seek to end
        # Write metadata segment
        meta_content = encode_metadata([g._to_group_def() for g in self._groups])
        self._write_segment(SegmentType.METADATA, meta_content, chunk_count=0)

    def _write_segment(self, seg_type: int, content: bytes, chunk_count: int) -> None:
        seg_start = self._file.tell()
        seg = SegmentHeader(
            type=seg_type,
            flags=0,
            content_length=len(content),
            next_segment_offset=0,  # patched below
            chunk_count=chunk_count,
            crc32=0,
        )
        self._file.write(seg.to_bytes())
        self._file.write(content)
        next_off = self._file.tell()
        seg.next_segment_offset = next_off
        self._file.seek(seg_start)
        self._file.write(seg.to_bytes())
        self._file.seek(next_off)
        self._segment_count += 1

    def _write_data_segment(self, pending: list) -> None:
        parts = [struct.pack("<i", len(pending))]
        for global_idx, ch in pending:
            raw = ch._to_bytes()
            parts.append(struct.pack(CHUNK_HEADER_FMT, global_idx, ch.sample_count, len(raw)))
            parts.append(raw)
        self._write_segment(SegmentType.DATA, b"".join(parts), chunk_count=len(pending))

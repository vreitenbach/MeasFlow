"""Writer for the .meas binary format."""

from __future__ import annotations

import io
import struct
import time
from typing import Any

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

    def __init__(self, name: str, dtype: MeasDataType, index: int) -> None:
        self.name = name
        self.data_type = dtype
        self.properties: dict[str, Any] = {}
        self._index = index
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
        ch = ChannelWriter(name, dtype, len(self._channels))
        self._channels.append(ch)
        return ch

    def _to_group_def(self) -> GroupDef:
        props = {
            k: (v if isinstance(v, MeasValue) else MeasValue.from_python(v))
            for k, v in self.properties.items()
        }
        return GroupDef(self.name, props, [ch._to_channel_def() for ch in self._channels])


class MeasWriter:
    """Write a .meas file. Use as a context manager or call close() explicitly."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._groups: list[GroupWriter] = []

    def add_group(self, name: str) -> GroupWriter:
        """Add a measurement group. Returns a GroupWriter to add channels to."""
        g = GroupWriter(name)
        self._groups.append(g)
        return g

    def __enter__(self) -> "MeasWriter":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Finalise and write the file to disk."""
        self._write_file()

    def _write_file(self) -> None:
        created_ns = int(time.time() * 1_000_000_000)

        # ── Encode metadata ──────────────────────────────────────────────
        group_defs = [g._to_group_def() for g in self._groups]
        meta_content = encode_metadata(group_defs)

        # ── Flatten all channels (global index across all groups) ────────
        all_channels: list[ChannelWriter] = []
        for g in self._groups:
            all_channels.extend(g._channels)
        non_empty = [(i, ch) for i, ch in enumerate(all_channels) if ch.sample_count > 0]

        # ── Compute offsets upfront ──────────────────────────────────────
        meta_seg_offset = FILE_HEADER_SIZE  # 64
        data_seg_offset = meta_seg_offset + SEGMENT_HEADER_SIZE + len(meta_content)

        has_data = bool(non_empty)

        # ── Build data content: [int32 chunkCount] + chunks ──────────────
        data_content = b""
        if has_data:
            chunk_count = len(non_empty)
            parts = [struct.pack("<i", chunk_count)]
            for i, ch in non_empty:
                raw = ch._to_bytes()
                parts.append(struct.pack(CHUNK_HEADER_FMT, i, ch.sample_count, len(raw)))
                parts.append(raw)
            data_content = b"".join(parts)

        data_seg_end = data_seg_offset + SEGMENT_HEADER_SIZE + len(data_content)

        # ── Assemble file ────────────────────────────────────────────────
        buf = io.BytesIO()

        segment_count = 1 + (1 if has_data else 0)
        hdr = FileHeader(created_at_nanos=created_ns, segment_count=segment_count)
        buf.write(hdr.to_bytes())

        # Metadata segment
        meta_seg = SegmentHeader(
            type=SegmentType.METADATA,
            flags=0,
            content_length=len(meta_content),
            next_segment_offset=data_seg_offset if has_data else data_seg_offset,
            chunk_count=0,
            crc32=0,
        )
        buf.write(meta_seg.to_bytes())
        buf.write(meta_content)

        # Data segment (if any)
        if has_data:
            data_seg = SegmentHeader(
                type=SegmentType.DATA,
                flags=0,
                content_length=len(data_content),
                next_segment_offset=data_seg_end,
                chunk_count=len(non_empty),
                crc32=0,
            )
            buf.write(data_seg.to_bytes())
            buf.write(data_content)

        with open(self._path, "wb") as f:
            f.write(buf.getvalue())

"""Reader for the .meas binary format."""

from __future__ import annotations

import struct
from typing import Any

import numpy as np

from measflow.types import MeasDataType, MeasTimestamp, MeasValue, _TYPE_NUMPY
from measflow._codec import (
    FileHeader,
    SegmentHeader,
    SegmentType,
    GroupDef,
    decode_metadata,
    decode_chunk_header,
    FILE_HEADER_SIZE,
    SEGMENT_HEADER_SIZE,
)


class MeasChannel:
    """A single typed channel within a group."""

    def __init__(
        self,
        name: str,
        dtype: MeasDataType,
        properties: dict[str, MeasValue],
        chunks: list[tuple[int, bytes]],
    ) -> None:
        self.name = name
        self.data_type = dtype
        self.properties = properties
        self._chunks = chunks  # list of (sample_count, raw_bytes)

    @property
    def sample_count(self) -> int:
        return sum(n for n, _ in self._chunks)

    def read_all(self) -> np.ndarray:
        """Return all samples as a numpy array."""
        if not self._chunks:
            dtype = _TYPE_NUMPY.get(self.data_type, "<i8")
            return np.array([], dtype=dtype)
        if self.data_type not in _TYPE_NUMPY:
            raise ValueError(f"Cannot decode channel type {self.data_type!r}")
        parts = [np.frombuffer(raw, dtype=_TYPE_NUMPY[self.data_type]) for _, raw in self._chunks]
        return np.concatenate(parts) if len(parts) > 1 else parts[0].copy()

    def read_timestamps(self) -> list[MeasTimestamp]:
        """Read all samples as MeasTimestamp objects (Timestamp channels only)."""
        if self.data_type != MeasDataType.Timestamp:
            raise ValueError(f"Channel '{self.name}' has type {self.data_type.name}, not Timestamp")
        return [MeasTimestamp(int(v)) for v in self.read_all()]

    def __repr__(self) -> str:
        return f"MeasChannel({self.name!r}, {self.data_type.name}, samples={self.sample_count})"


class MeasGroup:
    """A named group containing one or more channels."""

    def __init__(
        self,
        name: str,
        properties: dict[str, MeasValue],
        channels: list[MeasChannel],
    ) -> None:
        self.name = name
        self.properties = properties
        self.channels = channels
        self._by_name = {ch.name: ch for ch in channels}

    def __getitem__(self, name: str) -> MeasChannel:
        if name not in self._by_name:
            raise KeyError(f"Channel '{name}' not found in group '{self.name}'")
        return self._by_name[name]

    def __repr__(self) -> str:
        return f"MeasGroup({self.name!r}, channels={[ch.name for ch in self.channels]})"


class MeasReader:
    """Read a .meas file. Use as a context manager or construct directly."""

    def __init__(self, path: str) -> None:
        self._path = path
        self.groups: list[MeasGroup] = []
        self.created_at: MeasTimestamp | None = None
        self._by_name: dict[str, MeasGroup] = {}
        self._read()

    def __enter__(self) -> "MeasReader":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def __getitem__(self, name: str) -> MeasGroup:
        if name not in self._by_name:
            raise KeyError(f"Group '{name}' not found")
        return self._by_name[name]

    def _read(self) -> None:
        with open(self._path, "rb") as f:
            data = f.read()

        file_hdr = FileHeader.from_bytes(data)
        self.created_at = MeasTimestamp(file_hdr.created_at_nanos)

        # channel_index → list of (sample_count, raw_bytes)
        channel_chunks: dict[int, list[tuple[int, bytes]]] = {}
        group_defs: list[GroupDef] = []

        offset = file_hdr.first_segment_offset
        while 0 < offset < len(data):
            if offset + SEGMENT_HEADER_SIZE > len(data):
                break
            seg = SegmentHeader.from_bytes(data[offset:])
            content_start = offset + SEGMENT_HEADER_SIZE
            content_end = content_start + seg.content_length
            if content_end > len(data):
                break
            content = bytes(data[content_start:content_end])

            if seg.type == SegmentType.METADATA:
                group_defs = decode_metadata(content)
            elif seg.type == SegmentType.DATA:
                pos = 0
                # Data content begins with [int32: chunkCount]
                (chunk_count,) = struct.unpack_from("<i", content, pos)
                pos += 4
                for _ in range(chunk_count):
                    ch_idx, sample_count, data_len, pos = decode_chunk_header(content, pos)
                    raw = content[pos : pos + data_len]
                    pos += data_len
                    channel_chunks.setdefault(ch_idx, []).append((sample_count, raw))

            next_off = seg.next_segment_offset
            if next_off <= offset:
                break
            offset = next_off

        # Build groups/channels from defs + accumulated chunk data
        global_idx = 0
        for gdef in group_defs:
            channels = []
            for chdef in gdef.channels:
                chunks = channel_chunks.get(global_idx, [])
                channels.append(MeasChannel(chdef.name, chdef.data_type, chdef.properties, chunks))
                global_idx += 1
            grp = MeasGroup(gdef.name, gdef.properties, channels)
            self.groups.append(grp)
            self._by_name[gdef.name] = grp

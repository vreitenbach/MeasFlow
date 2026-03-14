"""Bus metadata types and binary encoding for MEAS §10.

Bus channel definitions are stored as a Binary property ``MEAS.bus_def`` on a
group.  Use :func:`encode_bus_def` / :func:`decode_bus_def` to convert between
:class:`BusChannelDefinition` objects and raw bytes.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

from measflow._codec import read_string_from, write_string_bytes


# ── Enumerations ──────────────────────────────────────────────────────────────

class BusType(IntEnum):
    NONE = 0
    CAN = 1
    CAN_FD = 2
    LIN = 3
    FLEX_RAY = 4
    ETHERNET = 5
    MOST = 6


class FrameDirection(IntEnum):
    RX = 0
    TX = 1
    TX_RQ = 2


class ByteOrder(IntEnum):
    INTEL = 0    # Little-endian / LSB
    MOTOROLA = 1  # Big-endian / MSB (CAN DBC)


class SignalDataType(IntEnum):
    UNSIGNED = 0
    SIGNED = 1
    FLOAT32 = 2
    FLOAT64 = 3


class LinChecksumType(IntEnum):
    CLASSIC = 0
    ENHANCED = 1


class FlexRayChannel(IntEnum):
    A = 0
    B = 1
    AB = 2


class E2EProfile(IntEnum):
    PROFILE_01 = 1
    PROFILE_02 = 2
    PROFILE_04 = 4
    PROFILE_05 = 5
    PROFILE_06 = 6
    PROFILE_07 = 7
    PROFILE_08 = 8
    PROFILE_11 = 11
    JLR = 0xFF


class SecOcAlgorithm(IntEnum):
    CMAC_AES128 = 0
    CMAC_AES256 = 1
    HMAC_SHA256 = 2
    HMAC_SHA384 = 3


class FreshnessValueType(IntEnum):
    COUNTER = 0
    TIMESTAMP = 1
    BOTH = 2


# ── Bus Configs ───────────────────────────────────────────────────────────────

@dataclass
class BusConfig:
    bus_type: BusType = BusType.NONE


@dataclass
class CanBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.CAN, init=False)
    is_extended_id: bool = False
    baud_rate: int = 500_000


@dataclass
class CanFdBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.CAN_FD, init=False)
    is_extended_id: bool = False
    arbitration_baud_rate: int = 500_000
    data_baud_rate: int = 2_000_000


@dataclass
class LinBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.LIN, init=False)
    baud_rate: int = 19_200
    lin_version: int = 2


@dataclass
class FlexRayBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.FLEX_RAY, init=False)
    cycle_time_us: int = 1000
    macroticks_per_cycle: int = 50


@dataclass
class EthernetBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.ETHERNET, init=False)


@dataclass
class MostBusConfig(BusConfig):
    bus_type: BusType = field(default=BusType.MOST, init=False)


# ── Support types ─────────────────────────────────────────────────────────────

@dataclass
class MultiplexCondition:
    mux_signal_name: str = ""
    low_value: int = 0
    high_value: int = 0
    parent_condition: Optional[MultiplexCondition] = None


@dataclass
class SignalDefinition:
    name: str = ""
    start_bit: int = 0
    bit_length: int = 8
    byte_order: ByteOrder = ByteOrder.INTEL
    data_type: SignalDataType = SignalDataType.UNSIGNED
    factor: float = 1.0
    offset: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unit: Optional[str] = None
    is_multiplexer: bool = False
    mux_condition: Optional[MultiplexCondition] = None
    value_descriptions: Optional[dict] = None  # dict[int, str]


@dataclass
class E2EProtection:
    profile: E2EProfile = E2EProfile.PROFILE_01
    crc_start_bit: int = 0
    crc_bit_length: int = 8
    counter_start_bit: int = 8
    counter_bit_length: int = 4
    data_id: int = 0
    crc_polynomial: int = 0


@dataclass
class SecOcConfig:
    algorithm: SecOcAlgorithm = SecOcAlgorithm.CMAC_AES128
    freshness_value_start_bit: int = 0
    freshness_value_truncated_length: int = 0
    freshness_value_full_length: int = 0
    freshness_type: FreshnessValueType = FreshnessValueType.COUNTER
    mac_start_bit: int = 0
    mac_truncated_length: int = 0
    mac_full_length: int = 0
    authentic_payload_length: int = 0
    data_id: int = 0
    auth_build_attempts: int = 1
    use_freshness_value_manager: bool = False
    key_id: int = 0


@dataclass
class MultiplexConfig:
    mux_signal_name: str = ""
    mux_groups: dict = field(default_factory=dict)  # dict[int, list[str]]


@dataclass
class ContainedPduDefinition:
    name: str = ""
    header_id: int = 0
    length: int = 0
    signals: list = field(default_factory=list)  # list[SignalDefinition]


@dataclass
class PduDefinition:
    name: str = ""
    pdu_id: int = 0
    byte_offset: int = 0
    length: int = 0
    is_container_pdu: bool = False
    e2e_protection: Optional[E2EProtection] = None
    sec_oc: Optional[SecOcConfig] = None
    multiplexing: Optional[MultiplexConfig] = None
    signals: list = field(default_factory=list)         # list[SignalDefinition]
    contained_pdus: list = field(default_factory=list)  # list[ContainedPduDefinition]


@dataclass
class FrameDefinition:
    name: str = ""
    frame_id: int = 0
    payload_length: int = 0
    direction: FrameDirection = FrameDirection.RX
    flags: int = 0  # FrameFlags bitmask
    signals: list = field(default_factory=list)  # list[SignalDefinition]
    pdus: list = field(default_factory=list)     # list[PduDefinition]


@dataclass
class CanFrameDefinition(FrameDefinition):
    is_extended_id: bool = False


@dataclass
class CanFdFrameDefinition(FrameDefinition):
    is_extended_id: bool = False
    bit_rate_switch: bool = False
    error_state_indicator: bool = False


@dataclass
class LinFrameDefinition(FrameDefinition):
    nad: int = 0
    checksum_type: LinChecksumType = LinChecksumType.ENHANCED


@dataclass
class FlexRayFrameDefinition(FrameDefinition):
    cycle_count: int = 0
    channel: FlexRayChannel = FlexRayChannel.A


@dataclass
class EthernetFrameDefinition(FrameDefinition):
    mac_source: bytes = field(default_factory=lambda: bytes(6))
    mac_destination: bytes = field(default_factory=lambda: bytes(6))
    vlan_id: int = 0
    ether_type: int = 0


@dataclass
class MostFrameDefinition(FrameDefinition):
    function_block: int = 0
    instance_id: int = 0
    function_id: int = 0


@dataclass
class ValueTable:
    name: str = ""
    entries: dict = field(default_factory=dict)  # dict[int, str]


@dataclass
class BusChannelDefinition:
    bus_config: BusConfig = field(default_factory=BusConfig)
    raw_frame_channel_name: str = ""
    timestamp_channel_name: str = ""
    frames: list = field(default_factory=list)        # list[FrameDefinition]
    value_tables: list = field(default_factory=list)  # list[ValueTable]


# ── Public encode / decode ────────────────────────────────────────────────────

_FORMAT_VERSION = 1


def encode_bus_def(defn: BusChannelDefinition) -> bytes:
    """Serialise a BusChannelDefinition to bytes for storage in MEAS.bus_def (§10)."""
    parts: list[bytes] = [bytes([_FORMAT_VERSION])]
    parts.append(_encode_bus_config(defn.bus_config))
    parts.append(write_string_bytes(defn.raw_frame_channel_name))
    parts.append(write_string_bytes(defn.timestamp_channel_name))
    parts.append(struct.pack("<i", len(defn.frames)))
    for frame in defn.frames:
        parts.append(_encode_frame(frame, defn.bus_config.bus_type))
    parts.append(struct.pack("<i", len(defn.value_tables)))
    for vt in defn.value_tables:
        parts.append(write_string_bytes(vt.name))
        parts.append(struct.pack("<i", len(vt.entries)))
        for val, desc in vt.entries.items():
            parts.append(struct.pack("<q", int(val)))
            parts.append(write_string_bytes(desc))
    return b"".join(parts)


def decode_bus_def(data: bytes) -> BusChannelDefinition:
    """Parse bytes from MEAS.bus_def into a BusChannelDefinition (§10)."""
    offset = 0
    version = data[offset]; offset += 1
    if version != _FORMAT_VERSION:
        raise ValueError(f"Unsupported bus metadata version {version}")

    bus_config, offset = _decode_bus_config(data, offset)
    raw_ch, offset = read_string_from(data, offset)
    ts_ch, offset = read_string_from(data, offset)

    (frame_count,) = struct.unpack_from("<i", data, offset); offset += 4
    frames = []
    for _ in range(frame_count):
        frame, offset = _decode_frame(data, offset, bus_config.bus_type)
        frames.append(frame)

    (vt_count,) = struct.unpack_from("<i", data, offset); offset += 4
    value_tables = []
    for _ in range(vt_count):
        name, offset = read_string_from(data, offset)
        (entry_count,) = struct.unpack_from("<i", data, offset); offset += 4
        entries: dict[int, str] = {}
        for _ in range(entry_count):
            (val,) = struct.unpack_from("<q", data, offset); offset += 8
            desc, offset = read_string_from(data, offset)
            entries[val] = desc
        value_tables.append(ValueTable(name=name, entries=entries))

    return BusChannelDefinition(
        bus_config=bus_config,
        raw_frame_channel_name=raw_ch,
        timestamp_channel_name=ts_ch,
        frames=frames,
        value_tables=value_tables,
    )


# ── Private helpers ───────────────────────────────────────────────────────────

def _b(value: bool) -> bytes:
    return bytes([1 if value else 0])


def _encode_bus_config(cfg: BusConfig) -> bytes:
    parts: list[bytes] = [bytes([int(cfg.bus_type)])]
    if isinstance(cfg, CanBusConfig):
        parts += [_b(cfg.is_extended_id), struct.pack("<i", cfg.baud_rate)]
    elif isinstance(cfg, CanFdBusConfig):
        parts += [_b(cfg.is_extended_id), struct.pack("<i", cfg.arbitration_baud_rate),
                  struct.pack("<i", cfg.data_baud_rate)]
    elif isinstance(cfg, LinBusConfig):
        parts += [struct.pack("<i", cfg.baud_rate), bytes([cfg.lin_version])]
    elif isinstance(cfg, FlexRayBusConfig):
        parts += [struct.pack("<i", cfg.cycle_time_us), struct.pack("<i", cfg.macroticks_per_cycle)]
    # Ethernet / MOST: no additional fields
    return b"".join(parts)


def _decode_bus_config(data: bytes, offset: int):
    bus_type = BusType(data[offset]); offset += 1
    if bus_type == BusType.CAN:
        is_ext = bool(data[offset]); offset += 1
        (baud,) = struct.unpack_from("<i", data, offset); offset += 4
        return CanBusConfig(is_extended_id=is_ext, baud_rate=baud), offset
    elif bus_type == BusType.CAN_FD:
        is_ext = bool(data[offset]); offset += 1
        (arb,) = struct.unpack_from("<i", data, offset); offset += 4
        (dat,) = struct.unpack_from("<i", data, offset); offset += 4
        return CanFdBusConfig(is_extended_id=is_ext, arbitration_baud_rate=arb, data_baud_rate=dat), offset
    elif bus_type == BusType.LIN:
        (baud,) = struct.unpack_from("<i", data, offset); offset += 4
        lin_ver = data[offset]; offset += 1
        return LinBusConfig(baud_rate=baud, lin_version=lin_ver), offset
    elif bus_type == BusType.FLEX_RAY:
        (cycle,) = struct.unpack_from("<i", data, offset); offset += 4
        (macros,) = struct.unpack_from("<i", data, offset); offset += 4
        return FlexRayBusConfig(cycle_time_us=cycle, macroticks_per_cycle=macros), offset
    elif bus_type == BusType.ETHERNET:
        return EthernetBusConfig(), offset
    elif bus_type == BusType.MOST:
        return MostBusConfig(), offset
    else:
        raise ValueError(f"Unknown bus type: {bus_type}")


def _encode_frame(frame: FrameDefinition, bus_type: BusType) -> bytes:
    parts: list[bytes] = [
        write_string_bytes(frame.name),
        struct.pack("<I", frame.frame_id),
        struct.pack("<i", frame.payload_length),
        bytes([int(frame.direction)]),
        struct.pack("<H", frame.flags),
    ]
    if isinstance(frame, CanFrameDefinition):
        parts.append(_b(frame.is_extended_id))
    elif isinstance(frame, CanFdFrameDefinition):
        parts += [_b(frame.is_extended_id), _b(frame.bit_rate_switch), _b(frame.error_state_indicator)]
    elif isinstance(frame, LinFrameDefinition):
        parts += [bytes([frame.nad]), bytes([int(frame.checksum_type)])]
    elif isinstance(frame, FlexRayFrameDefinition):
        parts += [bytes([frame.cycle_count]), bytes([int(frame.channel)])]
    elif isinstance(frame, EthernetFrameDefinition):
        parts += [
            (frame.mac_source + bytes(6))[:6],
            (frame.mac_destination + bytes(6))[:6],
            struct.pack("<H", frame.vlan_id),
            struct.pack("<H", frame.ether_type),
        ]
    elif isinstance(frame, MostFrameDefinition):
        parts += [struct.pack("<H", frame.function_block), bytes([frame.instance_id]),
                  struct.pack("<H", frame.function_id)]
    parts.append(struct.pack("<i", len(frame.signals)))
    for sig in frame.signals:
        parts.append(_encode_signal(sig))
    parts.append(struct.pack("<i", len(frame.pdus)))
    for pdu in frame.pdus:
        parts.append(_encode_pdu(pdu))
    return b"".join(parts)


def _decode_frame(data: bytes, offset: int, bus_type: BusType):
    name, offset = read_string_from(data, offset)
    (frame_id,) = struct.unpack_from("<I", data, offset); offset += 4
    (payload_len,) = struct.unpack_from("<i", data, offset); offset += 4
    direction = FrameDirection(data[offset]); offset += 1
    (flags,) = struct.unpack_from("<H", data, offset); offset += 2

    base = dict(name=name, frame_id=frame_id, payload_length=payload_len,
                direction=direction, flags=flags)
    if bus_type == BusType.CAN:
        is_ext = bool(data[offset]); offset += 1
        frame: FrameDefinition = CanFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags, is_extended_id=is_ext)
    elif bus_type == BusType.CAN_FD:
        is_ext = bool(data[offset]); offset += 1
        brs = bool(data[offset]); offset += 1
        esi = bool(data[offset]); offset += 1
        frame = CanFdFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags,
            is_extended_id=is_ext, bit_rate_switch=brs, error_state_indicator=esi)
    elif bus_type == BusType.LIN:
        nad = data[offset]; offset += 1
        cs_type = LinChecksumType(data[offset]); offset += 1
        frame = LinFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags, nad=nad, checksum_type=cs_type)
    elif bus_type == BusType.FLEX_RAY:
        cycle = data[offset]; offset += 1
        ch = FlexRayChannel(data[offset]); offset += 1
        frame = FlexRayFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags, cycle_count=cycle, channel=ch)
    elif bus_type == BusType.ETHERNET:
        mac_src = bytes(data[offset:offset + 6]); offset += 6
        mac_dst = bytes(data[offset:offset + 6]); offset += 6
        (vlan,) = struct.unpack_from("<H", data, offset); offset += 2
        (etype,) = struct.unpack_from("<H", data, offset); offset += 2
        frame = EthernetFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags,
            mac_source=mac_src, mac_destination=mac_dst, vlan_id=vlan, ether_type=etype)
    elif bus_type == BusType.MOST:
        (fb,) = struct.unpack_from("<H", data, offset); offset += 2
        inst = data[offset]; offset += 1
        (fid,) = struct.unpack_from("<H", data, offset); offset += 2
        frame = MostFrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags,
            function_block=fb, instance_id=inst, function_id=fid)
    else:
        frame = FrameDefinition(
            name=name, frame_id=frame_id, payload_length=payload_len,
            direction=direction, flags=flags)

    (sig_count,) = struct.unpack_from("<i", data, offset); offset += 4
    for _ in range(sig_count):
        sig, offset = _decode_signal(data, offset)
        frame.signals.append(sig)
    (pdu_count,) = struct.unpack_from("<i", data, offset); offset += 4
    for _ in range(pdu_count):
        pdu, offset = _decode_pdu(data, offset)
        frame.pdus.append(pdu)
    return frame, offset


def _encode_signal(sig: SignalDefinition) -> bytes:
    parts: list[bytes] = [
        write_string_bytes(sig.name),
        struct.pack("<i", sig.start_bit),
        struct.pack("<i", sig.bit_length),
        bytes([int(sig.byte_order)]),
        bytes([int(sig.data_type)]),
        struct.pack("<d", sig.factor),
        struct.pack("<d", sig.offset),
    ]
    min_max_flags = (1 if sig.min_value is not None else 0) | (2 if sig.max_value is not None else 0)
    parts.append(bytes([min_max_flags]))
    if sig.min_value is not None:
        parts.append(struct.pack("<d", sig.min_value))
    if sig.max_value is not None:
        parts.append(struct.pack("<d", sig.max_value))
    parts.append(_b(sig.unit is not None))
    if sig.unit is not None:
        parts.append(write_string_bytes(sig.unit))
    parts.append(_b(sig.is_multiplexer))
    parts.append(_b(sig.mux_condition is not None))
    if sig.mux_condition is not None:
        parts.append(_encode_mux_condition(sig.mux_condition))
    vd = sig.value_descriptions or {}
    parts.append(struct.pack("<i", len(vd)))
    for val, desc in vd.items():
        parts.append(struct.pack("<q", int(val)))
        parts.append(write_string_bytes(desc))
    return b"".join(parts)


def _decode_signal(data: bytes, offset: int):
    name, offset = read_string_from(data, offset)
    (start_bit,) = struct.unpack_from("<i", data, offset); offset += 4
    (bit_len,) = struct.unpack_from("<i", data, offset); offset += 4
    byte_order = ByteOrder(data[offset]); offset += 1
    sig_dt = SignalDataType(data[offset]); offset += 1
    (factor,) = struct.unpack_from("<d", data, offset); offset += 8
    (sig_offset,) = struct.unpack_from("<d", data, offset); offset += 8
    min_max_flags = data[offset]; offset += 1
    min_val = None
    if min_max_flags & 1:
        (min_val,) = struct.unpack_from("<d", data, offset); offset += 8
    max_val = None
    if min_max_flags & 2:
        (max_val,) = struct.unpack_from("<d", data, offset); offset += 8
    has_unit = bool(data[offset]); offset += 1
    unit = None
    if has_unit:
        unit, offset = read_string_from(data, offset)
    is_mux = bool(data[offset]); offset += 1
    has_mux_cond = bool(data[offset]); offset += 1
    mux_cond = None
    if has_mux_cond:
        mux_cond, offset = _decode_mux_condition(data, offset)
    (vd_count,) = struct.unpack_from("<i", data, offset); offset += 4
    vd: dict[int, str] | None = {} if vd_count > 0 else None
    for _ in range(vd_count):
        (val,) = struct.unpack_from("<q", data, offset); offset += 8
        desc, offset = read_string_from(data, offset)
        vd[val] = desc  # type: ignore[index]
    sig = SignalDefinition(
        name=name, start_bit=start_bit, bit_length=bit_len,
        byte_order=byte_order, data_type=sig_dt,
        factor=factor, offset=sig_offset,
        min_value=min_val, max_value=max_val, unit=unit,
        is_multiplexer=is_mux, mux_condition=mux_cond,
        value_descriptions=vd if vd_count > 0 else None,
    )
    return sig, offset


def _encode_mux_condition(mc: MultiplexCondition) -> bytes:
    parts = [
        write_string_bytes(mc.mux_signal_name),
        struct.pack("<q", mc.low_value),
        struct.pack("<q", mc.high_value),
        _b(mc.parent_condition is not None),
    ]
    if mc.parent_condition is not None:
        parts.append(_encode_mux_condition(mc.parent_condition))
    return b"".join(parts)


def _decode_mux_condition(data: bytes, offset: int):
    name, offset = read_string_from(data, offset)
    (low,) = struct.unpack_from("<q", data, offset); offset += 8
    (high,) = struct.unpack_from("<q", data, offset); offset += 8
    has_parent = bool(data[offset]); offset += 1
    parent = None
    if has_parent:
        parent, offset = _decode_mux_condition(data, offset)
    return MultiplexCondition(mux_signal_name=name, low_value=low, high_value=high,
                              parent_condition=parent), offset


def _encode_pdu(pdu: PduDefinition) -> bytes:
    parts: list[bytes] = [
        write_string_bytes(pdu.name),
        struct.pack("<I", pdu.pdu_id),
        struct.pack("<i", pdu.byte_offset),
        struct.pack("<i", pdu.length),
        _b(pdu.is_container_pdu),
    ]
    parts.append(_b(pdu.e2e_protection is not None))
    if pdu.e2e_protection is not None:
        e = pdu.e2e_protection
        parts.append(bytes([int(e.profile)]))
        parts += [struct.pack("<i", e.crc_start_bit), struct.pack("<i", e.crc_bit_length),
                  struct.pack("<i", e.counter_start_bit), struct.pack("<i", e.counter_bit_length),
                  struct.pack("<I", e.data_id), struct.pack("<I", e.crc_polynomial)]
    parts.append(_b(pdu.sec_oc is not None))
    if pdu.sec_oc is not None:
        s = pdu.sec_oc
        parts.append(bytes([int(s.algorithm)]))
        parts += [struct.pack("<i", s.freshness_value_start_bit),
                  struct.pack("<i", s.freshness_value_truncated_length),
                  struct.pack("<i", s.freshness_value_full_length),
                  bytes([int(s.freshness_type)]),
                  struct.pack("<i", s.mac_start_bit),
                  struct.pack("<i", s.mac_truncated_length),
                  struct.pack("<i", s.mac_full_length),
                  struct.pack("<i", s.authentic_payload_length),
                  struct.pack("<I", s.data_id),
                  struct.pack("<i", s.auth_build_attempts),
                  _b(s.use_freshness_value_manager),
                  struct.pack("<I", s.key_id)]
    parts.append(_b(pdu.multiplexing is not None))
    if pdu.multiplexing is not None:
        m = pdu.multiplexing
        parts.append(write_string_bytes(m.mux_signal_name))
        parts.append(struct.pack("<i", len(m.mux_groups)))
        for val, names in m.mux_groups.items():
            parts.append(struct.pack("<q", int(val)))
            parts.append(struct.pack("<i", len(names)))
            for n in names:
                parts.append(write_string_bytes(n))
    parts.append(struct.pack("<i", len(pdu.signals)))
    for sig in pdu.signals:
        parts.append(_encode_signal(sig))
    parts.append(struct.pack("<i", len(pdu.contained_pdus)))
    for cpdu in pdu.contained_pdus:
        parts += [write_string_bytes(cpdu.name), struct.pack("<I", cpdu.header_id),
                  struct.pack("<i", cpdu.length), struct.pack("<i", len(cpdu.signals))]
        for sig in cpdu.signals:
            parts.append(_encode_signal(sig))
    return b"".join(parts)


def _decode_pdu(data: bytes, offset: int):
    name, offset = read_string_from(data, offset)
    (pdu_id,) = struct.unpack_from("<I", data, offset); offset += 4
    (byte_off,) = struct.unpack_from("<i", data, offset); offset += 4
    (length,) = struct.unpack_from("<i", data, offset); offset += 4
    is_container = bool(data[offset]); offset += 1

    e2e = None
    has_e2e = bool(data[offset]); offset += 1
    if has_e2e:
        profile = E2EProfile(data[offset]); offset += 1
        (crc_sb,) = struct.unpack_from("<i", data, offset); offset += 4
        (crc_bl,) = struct.unpack_from("<i", data, offset); offset += 4
        (cnt_sb,) = struct.unpack_from("<i", data, offset); offset += 4
        (cnt_bl,) = struct.unpack_from("<i", data, offset); offset += 4
        (did,) = struct.unpack_from("<I", data, offset); offset += 4
        (poly,) = struct.unpack_from("<I", data, offset); offset += 4
        e2e = E2EProtection(profile=profile, crc_start_bit=crc_sb, crc_bit_length=crc_bl,
                            counter_start_bit=cnt_sb, counter_bit_length=cnt_bl,
                            data_id=did, crc_polynomial=poly)

    sec_oc = None
    has_sec_oc = bool(data[offset]); offset += 1
    if has_sec_oc:
        algo = SecOcAlgorithm(data[offset]); offset += 1
        (fv_sb,) = struct.unpack_from("<i", data, offset); offset += 4
        (fv_tl,) = struct.unpack_from("<i", data, offset); offset += 4
        (fv_fl,) = struct.unpack_from("<i", data, offset); offset += 4
        ft = FreshnessValueType(data[offset]); offset += 1
        (mac_sb,) = struct.unpack_from("<i", data, offset); offset += 4
        (mac_tl,) = struct.unpack_from("<i", data, offset); offset += 4
        (mac_fl,) = struct.unpack_from("<i", data, offset); offset += 4
        (apl,) = struct.unpack_from("<i", data, offset); offset += 4
        (did,) = struct.unpack_from("<I", data, offset); offset += 4
        (aba,) = struct.unpack_from("<i", data, offset); offset += 4
        use_fvm = bool(data[offset]); offset += 1
        (kid,) = struct.unpack_from("<I", data, offset); offset += 4
        sec_oc = SecOcConfig(algorithm=algo, freshness_value_start_bit=fv_sb,
                             freshness_value_truncated_length=fv_tl, freshness_value_full_length=fv_fl,
                             freshness_type=ft, mac_start_bit=mac_sb, mac_truncated_length=mac_tl,
                             mac_full_length=mac_fl, authentic_payload_length=apl, data_id=did,
                             auth_build_attempts=aba, use_freshness_value_manager=use_fvm, key_id=kid)

    mux = None
    has_mux = bool(data[offset]); offset += 1
    if has_mux:
        mux_name, offset = read_string_from(data, offset)
        (gc,) = struct.unpack_from("<i", data, offset); offset += 4
        mux_groups: dict[int, list[str]] = {}
        for _ in range(gc):
            (val,) = struct.unpack_from("<q", data, offset); offset += 8
            (nc,) = struct.unpack_from("<i", data, offset); offset += 4
            names = []
            for _ in range(nc):
                n, offset = read_string_from(data, offset)
                names.append(n)
            mux_groups[val] = names
        mux = MultiplexConfig(mux_signal_name=mux_name, mux_groups=mux_groups)

    pdu = PduDefinition(name=name, pdu_id=pdu_id, byte_offset=byte_off, length=length,
                        is_container_pdu=is_container, e2e_protection=e2e, sec_oc=sec_oc,
                        multiplexing=mux)
    (sig_count,) = struct.unpack_from("<i", data, offset); offset += 4
    for _ in range(sig_count):
        sig, offset = _decode_signal(data, offset)
        pdu.signals.append(sig)
    (cpdu_count,) = struct.unpack_from("<i", data, offset); offset += 4
    for _ in range(cpdu_count):
        cname, offset = read_string_from(data, offset)
        (hid,) = struct.unpack_from("<I", data, offset); offset += 4
        (clen,) = struct.unpack_from("<i", data, offset); offset += 4
        cpdu = ContainedPduDefinition(name=cname, header_id=hid, length=clen)
        (csig_count,) = struct.unpack_from("<i", data, offset); offset += 4
        for _ in range(csig_count):
            sig, offset = _decode_signal(data, offset)
            cpdu.signals.append(sig)
        pdu.contained_pdus.append(cpdu)
    return pdu, offset

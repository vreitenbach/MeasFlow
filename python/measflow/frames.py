"""Wire-format encode/decode for raw bus frames stored in Binary channels (§11)."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field


# ── CAN / CAN-FD ──────────────────────────────────────────────────────────────
# Total: 6 + dlc bytes
# [uint32: arbitrationId][byte: dlc][byte: flags][bytes: payload]
# Flags: Bit 0 = BRS, Bit 1 = ESI, Bit 2 = ExtendedId

@dataclass
class CanFrame:
    arb_id: int
    dlc: int
    payload: bytes
    flags: int = 0  # Bit 0: BRS, Bit 1: ESI, Bit 2: ExtendedId

    def encode(self) -> bytes:
        pl = (self.payload + bytes(self.dlc))[:self.dlc]
        return struct.pack("<IBB", self.arb_id, self.dlc, self.flags) + pl

    @classmethod
    def decode(cls, data: bytes) -> CanFrame:
        arb_id, dlc, flags = struct.unpack_from("<IBB", data)
        payload = bytes(data[6:6 + dlc])
        return cls(arb_id=arb_id, dlc=dlc, payload=payload, flags=flags)

    @property
    def is_extended(self) -> bool:
        return bool(self.flags & 4)

    @property
    def bit_rate_switch(self) -> bool:
        return bool(self.flags & 1)

    @property
    def error_state_indicator(self) -> bool:
        return bool(self.flags & 2)


# ── LIN ───────────────────────────────────────────────────────────────────────
# Total: 4 + dlc bytes
# [byte: frameId][byte: dlc][byte: nad][byte: checksumType][bytes: payload]

@dataclass
class LinFrame:
    frame_id: int       # 6-bit LIN frame identifier (0-63)
    dlc: int
    payload: bytes
    nad: int = 0        # Node Address for Diagnostic
    checksum_type: int = 1  # 0=Classic, 1=Enhanced

    def encode(self) -> bytes:
        pl = (self.payload + bytes(self.dlc))[:self.dlc]
        return bytes([self.frame_id & 0x3F, self.dlc, self.nad, self.checksum_type]) + pl

    @classmethod
    def decode(cls, data: bytes) -> LinFrame:
        frame_id, dlc, nad, cs_type = data[0], data[1], data[2], data[3]
        payload = bytes(data[4:4 + dlc])
        return cls(frame_id=frame_id, dlc=dlc, payload=payload, nad=nad, checksum_type=cs_type)


# ── FlexRay ───────────────────────────────────────────────────────────────────
# Total: 6 + payloadLength bytes
# [uint16: slotId][byte: cycleCount][byte: channelFlags][uint16: payloadLength][bytes: payload]

@dataclass
class FlexRayFrame:
    slot_id: int
    payload: bytes
    cycle_count: int = 0
    channel_flags: int = 0  # Bit 0: ChA, Bit 1: ChB

    def encode(self) -> bytes:
        return struct.pack("<HBBH", self.slot_id, self.cycle_count,
                           self.channel_flags, len(self.payload)) + self.payload

    @classmethod
    def decode(cls, data: bytes) -> FlexRayFrame:
        slot_id, cycle, ch_flags, payload_len = struct.unpack_from("<HBBH", data)
        payload = bytes(data[6:6 + payload_len])
        return cls(slot_id=slot_id, payload=payload, cycle_count=cycle, channel_flags=ch_flags)


# ── Ethernet ──────────────────────────────────────────────────────────────────
# Total: 18 + payloadLength bytes
# [6B: macDestination][6B: macSource][uint16: etherType][uint16: vlanId]
# [uint16: payloadLength][bytes: payload]

@dataclass
class EthernetFrame:
    mac_destination: bytes
    mac_source: bytes
    ether_type: int
    payload: bytes
    vlan_id: int = 0

    def encode(self) -> bytes:
        mac_dst = (self.mac_destination + bytes(6))[:6]
        mac_src = (self.mac_source + bytes(6))[:6]
        return (mac_dst + mac_src
                + struct.pack("<HHH", self.ether_type, self.vlan_id, len(self.payload))
                + self.payload)

    @classmethod
    def decode(cls, data: bytes) -> EthernetFrame:
        mac_dst = bytes(data[0:6])
        mac_src = bytes(data[6:12])
        ether_type, vlan_id, payload_len = struct.unpack_from("<HHH", data, 12)
        payload = bytes(data[18:18 + payload_len])
        return cls(mac_destination=mac_dst, mac_source=mac_src, ether_type=ether_type,
                   payload=payload, vlan_id=vlan_id)

"""H.264 Annex B helpers: NAL splitting, IDR detection and idr_pic_id parsing (spec §9, 11)."""

from __future__ import annotations

NAL_IDR = 5
NAL_SPS = 7
NAL_PPS = 8

_HIGH_PROFILES = frozenset({100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135})


def nal_units(au: bytes) -> list[tuple[int, bytes]]:
    """Split an Annex B access unit into (nal_unit_type, nal bytes incl. header, no start code)."""
    starts: list[tuple[int, int]] = []  # (start code position, payload position)
    i = 0
    while True:
        i = au.find(b"\x00\x00\x01", i)
        if i < 0:
            break
        pos = i - 1 if i > 0 and au[i - 1] == 0 else i
        starts.append((pos, i + 3))
        i += 3
    out: list[tuple[int, bytes]] = []
    for k, (_, payload) in enumerate(starts):
        end = starts[k + 1][0] if k + 1 < len(starts) else len(au)
        nal = au[payload:end].rstrip(b"\x00")
        if nal:
            out.append((nal[0] & 0x1F, nal))
    return out


def is_idr(au: bytes) -> bool:
    return any(t == NAL_IDR for t, _ in nal_units(au))


def has_parameter_sets(au: bytes) -> bool:
    types = {t for t, _ in nal_units(au)}
    return NAL_SPS in types and NAL_PPS in types


def _unescape(rbsp: bytes) -> bytes:
    """Remove emulation prevention bytes (00 00 03 -> 00 00)."""
    out = bytearray()
    zeros = 0
    for b in rbsp:
        if zeros >= 2 and b == 3:
            zeros = 0
            continue
        out.append(b)
        zeros = zeros + 1 if b == 0 else 0
    return bytes(out)


class _Bits:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def u(self, n: int) -> int:
        v = 0
        for _ in range(n):
            byte = self.pos >> 3
            if byte >= len(self.data):
                raise ValueError("bitstream truncated")
            v = (v << 1) | ((self.data[byte] >> (7 - (self.pos & 7))) & 1)
            self.pos += 1
        return v

    def ue(self) -> int:
        zeros = 0
        while self.u(1) == 0:
            zeros += 1
            if zeros > 32:
                raise ValueError("bad exp-golomb code")
        return (1 << zeros) - 1 + (self.u(zeros) if zeros else 0)

    def se(self) -> int:
        k = self.ue()
        return (k + 1) // 2 if k & 1 else -(k // 2)


def _skip_scaling_list(b: _Bits, size: int) -> None:
    last = nxt = 8
    for _ in range(size):
        if nxt != 0:
            nxt = (last + b.se()) % 256
        if nxt != 0:
            last = nxt


def _parse_sps(nal: bytes) -> tuple[int, bool, bool]:
    """Return (log2_max_frame_num, frame_mbs_only_flag, separate_colour_plane_flag)."""
    b = _Bits(_unescape(nal[1:]))
    profile = b.u(8)
    b.u(16)  # constraint flags + level_idc
    b.ue()  # sps_id
    separate = False
    if profile in _HIGH_PROFILES:
        chroma = b.ue()
        if chroma == 3:
            separate = bool(b.u(1))
        b.ue()  # bit_depth_luma_minus8
        b.ue()  # bit_depth_chroma_minus8
        b.u(1)  # qpprime_y_zero_transform_bypass_flag
        if b.u(1):  # seq_scaling_matrix_present_flag
            for i in range(8 if chroma != 3 else 12):
                if b.u(1):
                    _skip_scaling_list(b, 16 if i < 6 else 64)
    log2_frame_num = b.ue() + 4
    poc_type = b.ue()
    if poc_type == 0:
        b.ue()
    elif poc_type == 1:
        b.u(1)
        b.se()
        b.se()
        for _ in range(b.ue()):
            b.se()
    b.ue()  # max_num_ref_frames
    b.u(1)  # gaps_in_frame_num_value_allowed_flag
    b.ue()  # pic_width_in_mbs_minus1
    b.ue()  # pic_height_in_map_units_minus1
    frame_mbs_only = bool(b.u(1))
    return log2_frame_num, frame_mbs_only, separate


def idr_pic_id(au: bytes) -> int | None:
    """idr_pic_id of the IDR slice in ``au``, or None when there is no IDR slice or no SPS to parse it."""
    units = nal_units(au)
    sps = next((n for t, n in units if t == NAL_SPS), None)
    slice_nal = next((n for t, n in units if t == NAL_IDR), None)
    if sps is None or slice_nal is None:
        return None
    try:
        log2_frame_num, frame_mbs_only, separate = _parse_sps(sps)
        b = _Bits(_unescape(slice_nal[1:]))
        b.ue()  # first_mb_in_slice
        b.ue()  # slice_type
        b.ue()  # pps_id
        if separate:
            b.u(2)  # colour_plane_id
        b.u(log2_frame_num)  # frame_num
        if not frame_mbs_only and b.u(1):  # field_pic_flag
            b.u(1)  # bottom_field_flag
        return b.ue()
    except ValueError:
        return None

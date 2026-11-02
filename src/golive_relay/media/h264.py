"""H.264 Annex B helpers: NAL splitting, IDR detection and idr_pic_id parsing (spec §9, 11)."""

from __future__ import annotations

from typing import NamedTuple

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


class _BitWriter:
    def __init__(self) -> None:
        self.bits: list[int] = []

    def copy(self, data: bytes, nbits: int) -> None:
        for p in range(nbits):
            self.bits.append((data[p >> 3] >> (7 - (p & 7))) & 1)

    def u(self, n: int, v: int) -> None:
        self.bits.extend((v >> (n - 1 - i)) & 1 for i in range(n))

    def ue(self, v: int) -> None:
        v += 1
        n = v.bit_length()
        self.u(n - 1, 0)
        self.u(n, v)

    def finish(self) -> bytes:
        """Append rbsp_stop_one_bit, zero-align and return the bytes."""
        self.bits.append(1)
        self.bits.extend([0] * (-len(self.bits) % 8))
        return bytes(int("".join(map(str, self.bits[i:i + 8])), 2) for i in range(0, len(self.bits), 8))


def _escape(rbsp: bytes) -> bytes:
    """Insert emulation prevention bytes (00 00 0x -> 00 00 03 0x for x <= 3)."""
    out = bytearray()
    zeros = 0
    for b in rbsp:
        if zeros >= 2 and b <= 3:
            out.append(3)
            zeros = 0
        out.append(b)
        zeros = zeros + 1 if b == 0 else 0
    return bytes(out)


class _Sps(NamedTuple):
    vui_flag_pos: int  # bit position of vui_parameters_present_flag
    restriction_flag_pos: int | None  # bit position of bitstream_restriction_flag (None without VUI)
    restriction: bool
    reorder: int  # max_num_reorder_frames (0 when no restriction)
    dec_buf: int  # max_dec_frame_buffering (0 when no restriction)
    max_num_ref_frames: int
    width: int
    height: int


def _skip_hrd(b: _Bits) -> None:
    cpb_cnt = b.ue() + 1
    b.u(8)  # bit_rate_scale + cpb_size_scale
    for _ in range(cpb_cnt):
        b.ue()
        b.ue()
        b.u(1)
    b.u(20)  # delays and lengths


def _parse_sps_full(nal: bytes) -> _Sps:
    b = _Bits(_unescape(nal[1:]))
    profile = b.u(8)
    b.u(16)
    b.ue()
    chroma, separate = 1, False
    if profile in _HIGH_PROFILES:
        chroma = b.ue()
        if chroma == 3:
            separate = bool(b.u(1))
        b.ue()
        b.ue()
        b.u(1)
        if b.u(1):
            for i in range(8 if chroma != 3 else 12):
                if b.u(1):
                    _skip_scaling_list(b, 16 if i < 6 else 64)
    b.ue()  # log2_max_frame_num_minus4
    poc_type = b.ue()
    if poc_type == 0:
        b.ue()
    elif poc_type == 1:
        b.u(1)
        b.se()
        b.se()
        for _ in range(b.ue()):
            b.se()
    num_ref = b.ue()
    b.u(1)
    mbs_w = b.ue() + 1
    map_h = b.ue() + 1
    fmo = b.u(1)
    if not fmo:
        b.u(1)  # mb_adaptive_frame_field_flag
    b.u(1)  # direct_8x8_inference_flag
    crop = (0, 0, 0, 0)
    if b.u(1):
        crop = (b.ue(), b.ue(), b.ue(), b.ue())
    if separate or chroma == 0:
        unit_x, unit_y = 1, 2 - fmo
    else:
        unit_x = 2 if chroma in (1, 2) else 1
        unit_y = (2 if chroma == 1 else 1) * (2 - fmo)
    width = mbs_w * 16 - unit_x * (crop[0] + crop[1])
    height = map_h * 16 * (2 - fmo) - unit_y * (crop[2] + crop[3])
    vui_pos = b.pos
    if not b.u(1):
        return _Sps(vui_pos, None, False, 0, 0, num_ref, width, height)
    if b.u(1):  # aspect_ratio_info_present_flag
        if b.u(8) == 255:
            b.u(32)
    if b.u(1):  # overscan_info_present_flag
        b.u(1)
    if b.u(1):  # video_signal_type_present_flag
        b.u(4)
        if b.u(1):
            b.u(24)
    if b.u(1):  # chroma_loc_info_present_flag
        b.ue()
        b.ue()
    if b.u(1):  # timing_info_present_flag
        b.u(32)
        b.u(32)
        b.u(1)
    hrd = b.u(1)
    if hrd:
        _skip_hrd(b)
    vcl = b.u(1)
    if vcl:
        _skip_hrd(b)
    if hrd or vcl:
        b.u(1)  # low_delay_hrd_flag
    b.u(1)  # pic_struct_present_flag
    flag_pos = b.pos
    if not b.u(1):
        return _Sps(vui_pos, flag_pos, False, 0, 0, num_ref, width, height)
    b.u(1)
    for _ in range(4):
        b.ue()
    reorder = b.ue()
    return _Sps(vui_pos, flag_pos, True, reorder, b.ue(), num_ref, width, height)


def _split_sps(data: bytes) -> bytes | None:
    if data[:3] == b"\x00\x00\x01" or data[:4] == b"\x00\x00\x00\x01":
        return next((n for t, n in nal_units(data) if t == NAL_SPS), None)
    return data


def sps_dimensions(au_or_sps: bytes) -> tuple[int, int] | None:
    """Cropped (width, height) from the SPS of an Annex B AU or a bare SPS NAL; None if absent/unparseable."""
    sps = _split_sps(au_or_sps)
    if sps is None:
        return None
    try:
        info = _parse_sps_full(sps)
    except ValueError:
        return None
    return info.width, info.height


def _rewrite_sps(nal: bytes) -> bytes:
    """Return ``nal`` with bitstream_restriction (reorder 0) added, or ``nal`` itself when already present."""
    info = _parse_sps_full(nal)
    if info.restriction:
        return nal
    rbsp = _unescape(nal[1:])
    w = _BitWriter()
    if info.restriction_flag_pos is None:
        w.copy(rbsp, info.vui_flag_pos)
        w.u(1, 1)  # vui_parameters_present_flag
        w.u(8, 0)  # aspect/overscan/video_signal/chroma_loc/timing/nal_hrd/vcl_hrd/pic_struct all absent
    else:
        w.copy(rbsp, info.restriction_flag_pos)
    w.u(1, 1)  # bitstream_restriction_flag
    w.u(1, 1)  # motion_vectors_over_pic_boundaries_flag
    w.ue(2)  # max_bytes_per_pic_denom
    w.ue(1)  # max_bits_per_mb_denom
    w.ue(16)  # log2_max_mv_length_horizontal
    w.ue(16)  # log2_max_mv_length_vertical
    w.ue(0)  # max_num_reorder_frames
    w.ue(info.max_num_ref_frames)  # max_dec_frame_buffering
    return nal[:1] + _escape(w.finish())


_last_rewrite: tuple[bytes, bytes] | None = None  # (input SPS, output SPS); called on every IDR


def ensure_low_delay_vui(au: bytes) -> bytes:
    """Make viewers' decoders output frames immediately: add VUI bitstream_restriction (reorder 0) to the SPS
    when it has none. Returns ``au`` itself when there is no SPS or nothing to change."""
    global _last_rewrite
    sps = next((n for t, n in nal_units(au) if t == NAL_SPS), None)
    if sps is None:
        return au
    if _last_rewrite is not None and _last_rewrite[0] == sps:
        new = _last_rewrite[1]
    else:
        try:
            new = _rewrite_sps(sps)
        except ValueError:
            return au
        _last_rewrite = (sps, new)
    if new is sps or new == sps:
        return au
    i = au.find(sps)
    return au[:i] + new + au[i + len(sps):]

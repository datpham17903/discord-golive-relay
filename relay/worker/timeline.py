"""RTP unwrapping and a shared presentation timeline for audio and video (spec §9)."""

from __future__ import annotations

from dataclasses import dataclass

_MOD = 1 << 32


class RtpUnwrapper:
    """Extends a 32-bit RTP timestamp to a monotonic-ish integer axis."""

    def __init__(self, clock_rate: int) -> None:
        self.clock_rate = clock_rate
        self._last_raw: int | None = None
        self._value = 0

    def unwrap(self, ts: int) -> int:
        ts &= _MOD - 1
        if self._last_raw is None:
            self._value = ts
        else:
            delta = (ts - self._last_raw) & (_MOD - 1)
            if delta >= _MOD // 2:
                delta -= _MOD  # reordered packet: step backwards
            self._value += delta
        self._last_raw = ts
        return self._value


@dataclass(slots=True)
class _Track:
    unwrapper: RtpUnwrapper
    anchor_ts: int
    anchor_mono: float
    from_rtcp: bool
    last_deadline: float = float("-inf")


class Timeline:
    """Maps (ssrc, RTP ts) to a monotonic presentation deadline.

    An RTCP-derived time anchors the track when present; otherwise the first
    arrival does, and results are flagged ``estimated`` (A/V sync is then only
    as good as arrival times).
    """

    def __init__(self, playout_delay_s: float = 0.1) -> None:
        self.playout_delay_s = playout_delay_s
        self._tracks: dict[int, _Track] = {}

    @property
    def estimated(self) -> bool:
        """True while no track exists or any track is still anchored by arrival time."""
        return not self._tracks or any(not t.from_rtcp for t in self._tracks.values())

    def map(self, ssrc: int, clock_rate: int, rtp_ts: int, mono_received: float,
            rtcp_time_mono: float | None) -> float:
        track = self._tracks.get(ssrc)
        if track is None:
            track = _Track(RtpUnwrapper(clock_rate), 0, mono_received, False)
            track.anchor_ts = track.unwrapper.unwrap(rtp_ts)
            self._tracks[ssrc] = track
            u = track.anchor_ts
        else:
            u = track.unwrapper.unwrap(rtp_ts)
        if rtcp_time_mono is not None:
            track.anchor_ts, track.anchor_mono, track.from_rtcp = u, rtcp_time_mono, True
        deadline = track.anchor_mono + (u - track.anchor_ts) / clock_rate + self.playout_delay_s
        deadline = max(deadline, track.last_deadline)
        track.last_deadline = deadline
        return deadline

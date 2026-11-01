"""Single send path: chooses between source content and slate (spec §6, 9, 11).

``next_video``/``next_audio`` are called from the library's player thread while
offers and control calls come from the main process, so all state is guarded by one lock.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Iterator

from discord.ext.native_voice.media import VideoFrame

from golive_relay.media.h264 import has_parameter_sets, idr_pic_id, is_idr
from golive_relay.media.slate import SlateGop
from golive_relay.worker.messages import AUDIO_FRAME_BYTES, EgressAudio, EgressVideo

MAX_EGRESS_AUS = 8  # ~133 ms at 60 fps; absorbs source bursts (6 dropped frames live), adds latency only while bursting
MAX_EGRESS_BYTES = 8 * 1024 * 1024
# The sender's pace equals the source rate, so a burst would leave a standing backlog (+~40 ms
# live). While CATCHUP_BACKLOG or more AUs stay queued, frames go out ~10% faster.
CATCHUP_BACKLOG = 2
CATCHUP_FACTOR = 0.9
STALL_S = 0.05  # a gap between sends that a viewer notices as a hitch
MAX_PCM_BLOCKS = 40  # 800 ms: audio is held as long as video sits in the 600 ms jitter buffer
_SILENCE = bytes(AUDIO_FRAME_BYTES)


class Publisher:
    def __init__(
        self,
        gate: Callable[[int, int, float], bool],
        clock: Callable[[], float] = time.monotonic,
        max_late_s: float = 0.2,
        on_need_idr: Callable[[], None] = lambda: None,
    ) -> None:
        self._gate = gate
        self._clock = clock
        self._max_late = max_late_s
        self._on_need_idr = on_need_idr
        self._lock = threading.Lock()
        self._source: tuple[int, int] | None = None
        self._slate: SlateGop | None = None
        self._slate_iter: Iterator[bytes] | None = None
        self._video: deque[EgressVideo] = deque()
        self._video_bytes = 0
        self._audio: deque[EgressAudio] = deque()
        self._await_idr = False  # discard source AUs until an IDR with SPS/PPS
        self._last_idr_id: int | None = None  # idr_pic_id of the last sent AU if it was an IDR, else None
        self._last_send: float | None = None
        self._window = {"sent": 0, "stalls": 0, "max_send_gap_ms": 0.0}

    # ------------------------------------------------------------ control

    def set_source(self, generation: int, epoch: int) -> None:
        with self._lock:
            self._purge_locked()
            self._source = (generation, epoch)
            self._await_idr = True

    def set_slate(self, slate: SlateGop | None) -> None:
        with self._lock:
            self._slate = slate
            self._slate_iter = None

    def close(self) -> None:
        with self._lock:
            self._purge_locked()
            self._source = None
            self._slate = None
            self._slate_iter = None

    def _purge_locked(self) -> None:
        self._video.clear()
        self._video_bytes = 0
        self._audio.clear()

    # ------------------------------------------------------------- offers

    def offer_video(self, au: EgressVideo) -> None:
        need = False
        with self._lock:
            if self._source != (au.generation, au.epoch):
                return
            if len(self._video) >= MAX_EGRESS_AUS or self._video_bytes + len(au.data) > MAX_EGRESS_BYTES:
                self._video.clear()
                self._video_bytes = 0
                self._await_idr = True
                need = True
            else:
                self._video.append(au)
                self._video_bytes += len(au.data)
        if need:
            self._on_need_idr()

    def offer_audio(self, au: EgressAudio) -> None:
        with self._lock:
            if self._source != (au.generation, au.epoch):
                return
            self._audio.append(au)
            while len(self._audio) > MAX_PCM_BLOCKS:
                self._audio.popleft()

    # -------------------------------------------------------------- reads

    def next_video(self) -> VideoFrame | None:
        with self._lock:
            frame, need = self._next_video_locked()
            if frame is not None:
                self._note_send_locked()
        if need:
            self._on_need_idr()
        return frame

    def _note_send_locked(self) -> None:
        now = self._clock()
        self._window["sent"] += 1
        if self._last_send is not None:
            gap = now - self._last_send
            self._window["stalls"] += gap > STALL_S
            self._window["max_send_gap_ms"] = max(self._window["max_send_gap_ms"], gap * 1000.0)
        self._last_send = now

    def take_window(self) -> dict[str, float]:
        """Send counters since the last call: sent, stalls (gaps over 50 ms), max_send_gap_ms."""
        with self._lock:
            w, self._window = self._window, {"sent": 0, "stalls": 0, "max_send_gap_ms": 0.0}
        return w

    def _gate_open_locked(self, now: float) -> bool:
        return self._source is not None and self._gate(self._source[0], self._source[1], now)

    def _next_video_locked(self) -> tuple[VideoFrame | None, bool]:
        now = self._clock()
        need = False
        if not self._gate_open_locked(now):
            # Anything queued is stale and the source must restart on an IDR.
            self._purge_locked()
            self._await_idr = True
            return self._next_slate_locked(), False
        self._slate_iter = None
        while self._video:
            au = self._video.popleft()
            self._video_bytes -= len(au.data)
            if now - au.deadline > self._max_late:
                self._await_idr = True
                need = True
                continue
            idr = is_idr(au.data)
            if self._await_idr:
                if not (idr and has_parameter_sets(au.data)):
                    need = True
                    continue
                self._await_idr = False
            if not idr:
                self._last_idr_id = None
                return VideoFrame(au.data, self._paced(au.frame_time_ms)), need
            new_id = idr_pic_id(au.data)
            if self._last_idr_id is not None and new_id == self._last_idr_id:
                pad = self._slate_p_frame()
                if pad is None:
                    self._await_idr = True
                    need = True
                    continue
                # Keep the IDR for the next call; one slate P-frame separates the equal ids.
                self._video.appendleft(au)
                self._video_bytes += len(au.data)
                self._last_idr_id = None
                return VideoFrame(pad, self._slate.frame_time_ms), need
            self._last_idr_id = new_id
            return VideoFrame(au.data, self._paced(au.frame_time_ms)), need
        return None, need

    def _paced(self, frame_time_ms: float) -> float:
        return frame_time_ms * CATCHUP_FACTOR if len(self._video) >= CATCHUP_BACKLOG else frame_time_ms

    def _slate_p_frame(self) -> bytes | None:
        if self._slate is None:
            return None
        for gop in self._slate.variants:
            if len(gop) > 1:
                return gop[1]
        return None

    def _next_slate_locked(self) -> VideoFrame | None:
        slate = self._slate
        if slate is None:
            return None
        au = next(self._slate_iter, None) if self._slate_iter is not None else None
        if au is None:
            self._slate_iter = slate.next_au(self._last_idr_id)
            au = next(self._slate_iter)
        self._last_idr_id = idr_pic_id(au) if is_idr(au) else None
        return VideoFrame(au, slate.frame_time_ms)

    def next_audio(self) -> bytes:
        with self._lock:
            now = self._clock()
            if not self._gate_open_locked(now):
                self._audio.clear()
                return _SILENCE
            while self._audio:
                if self._audio[0].deadline > now:
                    return _SILENCE  # hold until the shared-timeline deadline, like video
                au = self._audio.popleft()
                if now - au.deadline <= self._max_late:
                    return au.pcm
            return _SILENCE

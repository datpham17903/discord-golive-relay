"""MediaSource adapter over the Publisher for discord.ext.native_voice."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from discord.ext.native_voice.media import MediaSource, VideoConfig, VideoFrame

from golive_relay.media.publisher import Publisher


class RelayMediaSource(MediaSource):
    """Pull-based source: audio is PCM, or Opus in audio passthrough (silence when idle), video is {} when nothing
    is due."""

    def __init__(self, publisher: Publisher, video_config: VideoConfig) -> None:
        self._publisher = publisher
        self._video_config = video_config

    def is_opus(self) -> bool:
        return self._publisher.opus  # fixed per publisher, so constant for the play() session

    def read(self) -> bytes:
        return self._publisher.next_audio()

    # Frames are sent as soon as they are ready but never faster than frame_time (the worker's fixed
    # 1000/fps); {} is retried after 2 ms. Realtime pacing would stretch gaps and back up the sender.
    video_realtime = False
    video_retry_delay = 0.002

    def has_audio(self) -> bool:
        return True

    def has_video(self) -> bool:
        return True

    @property
    def video_config(self) -> VideoConfig:
        return self._video_config

    def read_video_streams(self, streams: Sequence) -> Mapping[str, VideoFrame]:
        if not streams:
            return {}
        frame = self._publisher.next_video()
        return {} if frame is None else {streams[0].rid: frame}

    def read_preview(self) -> None:
        return None

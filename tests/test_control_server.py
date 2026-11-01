import asyncio
import json

from relay.control.server import ControlServer


class _Writer:
    def __init__(self) -> None:
        self.data = b""
        self.closed = False

    def get_extra_info(self, name):
        return None

    def write(self, b: bytes) -> None:
        self.data += b

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def test_oversized_request_line_gets_json_error_and_closes(tmp_path) -> None:
    async def run() -> _Writer:
        reader = asyncio.StreamReader(limit=64)
        reader.feed_data(b"x" * 500 + b"\n")
        writer = _Writer()
        await ControlServer(tmp_path / "s", lambda r: {"ok": True})._serve(reader, writer)
        return writer

    w = asyncio.run(run())
    assert json.loads(w.data)["ok"] is False
    assert w.closed

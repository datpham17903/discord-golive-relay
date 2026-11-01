"""Local control socket: asyncio Unix socket, one JSON request line, one JSON response line."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

MAX_REQUEST_BYTES = 64 * 1024


def _peer_uid(writer: asyncio.StreamWriter) -> int | None:
    sock = writer.get_extra_info("socket")
    try:
        import socket
        import struct

        creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", creds)[1]
    except (AttributeError, OSError):
        return None


class ControlServer:
    """``handler(request: dict) -> dict`` may be sync or async; it runs on the event loop."""

    def __init__(self, socket_path: Path, handler: Callable[[dict], Any]) -> None:
        self._path = Path(socket_path)
        self._handler = handler
        self._server: asyncio.AbstractServer | None = None

    @classmethod
    def create(cls, socket_path: Path, handler: Callable[[dict], Any]) -> "ControlServer | None":
        """None where Unix sockets are not used (Windows)."""
        if sys.platform == "win32":
            return None
        return cls(socket_path, handler)

    async def start(self) -> None:
        if sys.platform == "win32":
            raise OSError("control socket is not available on Windows")
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        self._server = await asyncio.start_unix_server(self._serve, path=str(self._path))
        os.chmod(self._path, 0o660)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            except ValueError:  # request line exceeded the StreamReader limit
                line = None
            try:
                if line is None:
                    raise ValueError("request too large")
                req = json.loads(line[:MAX_REQUEST_BYTES])
                if not isinstance(req, dict):
                    raise ValueError("request must be an object")
                req["_peer_uid"] = _peer_uid(writer)  # never trust a client-supplied value
                resp = self._handler(req)
                if inspect.isawaitable(resp):
                    resp = await resp
            except Exception as exc:
                resp = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            writer.write((json.dumps(resp, default=str) + "\n").encode("utf-8"))
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()

import io
import json
import sys

import pytest

from relay.control.server import ControlServer
from relay.obs import JsonLogger, Metrics, write_status


def test_logger_redacts_secrets_everywhere():
    buf = io.StringIO()
    log = JsonLogger(buf, secrets=["s3cr3t-token"])
    log.event("login", detail="bad s3cr3t-token here", nested={"a": ["x s3cr3t-token"]}, token="whatever")
    line = buf.getvalue()
    assert "s3cr3t-token" not in line and "whatever" not in line
    rec = json.loads(line)
    assert rec["event"] == "login" and "ts" in rec
    assert rec["token"] == "***"


def test_logger_ignores_empty_secret():
    buf = io.StringIO()
    JsonLogger(buf, secrets=["", None]).event("x", a="abc")
    assert json.loads(buf.getvalue())["a"] == "abc"


def test_metrics_snapshot():
    m = Metrics()
    m.inc("retries")
    m.inc("retries", 2)
    m.gauge("queue", 5)
    m.timing("encode_ms", 10.0)
    m.timing("encode_ms", 30.0)
    snap = m.snapshot()
    assert snap["counters"]["retries"] == 3
    assert snap["gauges"]["queue"] == 5
    t = snap["timings"]["encode_ms"]
    assert t["count"] == 2 and t["max"] == 30.0 and t["avg"] == 20.0


def test_write_status_atomic(tmp_path):
    write_status(tmp_path, {"state": "idle"})
    assert json.loads((tmp_path / "status.json").read_text())["state"] == "idle"
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets only")
def test_control_server_roundtrip(tmp_path):
    import asyncio
    import os
    import stat

    from relay.control.cli import send_request

    async def go():
        sock = tmp_path / "c.sock"
        srv = ControlServer(sock, lambda req: {"ok": True, "echo": req["cmd"]})
        await srv.start()
        try:
            assert stat.S_IMODE(os.stat(sock).st_mode) == 0o660
            return await asyncio.get_running_loop().run_in_executor(None, send_request, sock, {"cmd": "status"})
        finally:
            await srv.close()

    assert asyncio.run(go()) == {"ok": True, "echo": "status"}


@pytest.mark.skipif(sys.platform != "win32", reason="windows only")
def test_control_server_unavailable_on_windows(tmp_path):
    assert ControlServer.create(tmp_path / "c.sock", lambda r: {}) is None

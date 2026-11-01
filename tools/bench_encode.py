"""Encode synthetic frames per profile (spec §14) and report latency, CPU time and RSS growth.

Usage: python tools/bench_encode.py [--seconds 10] [--profile NAME ...]
Synthetic content only; nothing is written to disk.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import av  # noqa: E402

from relay.worker.messages import OutputProfile  # noqa: E402
from relay.worker.process import _new_encoder  # noqa: E402

# name -> (width, height, fps, bitrate kbps) from the section 14 table (low end of the range)
PROFILES = {
    "saver": (854, 480, 30, 1200),
    "default": (1280, 720, 30, 2800),
    "detail": (1920, 1080, 30, 4500),
    "motion": (1920, 1080, 60, 7000),
}


def _rss_mib() -> float | None:
    """Current (not peak) resident set size; a process-wide peak cannot be attributed to one profile."""
    try:
        with open("/proc/self/statm") as f:  # Linux
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, AttributeError, ValueError):
        pass
    try:
        import ctypes
        from ctypes import wintypes

        class PMC(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (n, ctypes.c_size_t)
                for n in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                          "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")
            ]

        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.windll.psapi
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
        psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
        return pmc.WorkingSetSize / (1024 * 1024)
    except (AttributeError, OSError):
        return None


def bench(name: str, seconds: float) -> dict:
    w, h, fps, kbps = PROFILES[name]
    rss0 = _rss_mib()
    profile = OutputProfile("fixed", w, h, fps, w, h, kbps, 2.0, w * h, False)
    enc = _new_encoder(w, h, profile, "libx264")
    noise = os.urandom(w * h * 2)  # noise-like content; shifted per frame so every frame differs
    n = int(seconds * fps)
    times: list[float] = []
    size = 0
    cpu0 = time.process_time()
    for i in range(n):
        frame = av.VideoFrame(w, h, "yuv420p")
        off = (i * 64) % (w * h)
        for plane in frame.planes:
            plane.update(noise[off : off + plane.buffer_size])
        frame.pts = i
        t0 = time.perf_counter()
        for pkt in enc.encode(frame):
            size += pkt.size
        times.append((time.perf_counter() - t0) * 1000)
    cpu = time.process_time() - cpu0
    times.sort()
    return {
        "profile": f"{name} {w}x{h}@{fps}",
        "frames": n,
        "p50_ms": round(statistics.median(times), 2),
        "p95_ms": round(times[min(len(times) - 1, int(len(times) * 0.95))], 2),
        "budget_ms": round(1000 / fps, 2),
        "cpu_s": round(cpu, 2),
        "cpu_per_media_s": round(cpu / seconds, 2),
        "out_kbps": round(size * 8 / seconds / 1000),
        "rss_delta_mib": None if rss0 is None or (r := _rss_mib()) is None else round(r - rss0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seconds", type=float, default=10.0)
    ap.add_argument("--profile", action="append", choices=sorted(PROFILES))
    args = ap.parse_args()
    for name in args.profile or list(PROFILES):
        r = bench(name, args.seconds)
        print("  ".join(f"{k}={v}" for k, v in r.items()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

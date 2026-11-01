"""Derive source_dead_timeout_seconds (spec §10/18) from M0 spike logs.

Usage:
  python scripts/calibrate_tdead.py scripts/m0/out/g2c-*.jsonl [--min-minutes 10] [--force]
  python scripts/calibrate_tdead.py LOGS... --write --config config.yaml

Logs are JSON-lines (or *-console.txt copies; non-JSON lines are ignored). Each "stats" event
carries streams["<ssrc>/<audio|video>"].max_gap_s, which is cumulative per run, so the last value
per stream in a run is that run's maximum. Per-stream gaps are all that is logged, so the observed
gap is the conservative max over streams (the true gap with no packet of either kind is <= it).
Recommended T_dead = max(3.0, ceil_to_0.5(2 * observed_max_gap + 1.0)).
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "manifests" / "verified-capabilities.json"
MIN_T_DEAD = 3.0


def recommend(observed_max_gap_s: float) -> float:
    return max(MIN_T_DEAD, math.ceil((2 * observed_max_gap_s + 1.0) * 2) / 2)


def analyse_log(path: Path) -> dict | None:
    """Return {path, seconds, max_gaps{stream: s}} for one log, or None if it has no packets."""
    first_t = last_t = None
    gaps: dict[str, float] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if ev.get("event") != "stats" or not isinstance(ev.get("t"), (int, float)):
            continue
        streams = ev.get("streams") or {}
        if not any(s.get("packets", 0) > 0 for s in streams.values()):
            continue
        if first_t is None:
            first_t = ev["t"]
        last_t = ev["t"]
        for name, s in streams.items():
            gaps[name] = max(gaps.get(name, 0.0), float(s.get("max_gap_s", 0.0)))
    if first_t is None:
        return None
    return {"path": str(path), "seconds": last_t - first_t, "max_gaps": gaps}


def aggregate(runs: list[dict]) -> dict:
    max_gaps: dict[str, float] = {}
    for r in runs:
        for name, g in r["max_gaps"].items():
            key = name.split("/", 1)[-1]
            max_gaps[key] = max(max_gaps.get(key, 0.0), g)
    observed = max(max_gaps.values(), default=0.0)
    return {
        "runs": len(runs),
        "minutes": sum(r["seconds"] for r in runs) / 60.0,
        "max_gap_by_kind_s": {k: round(v, 4) for k, v in sorted(max_gaps.items())},
        "observed_max_gap_s": round(observed, 4),
        "recommended_t_dead_s": recommend(observed),
    }


def write_manifest(path: Path, result: dict, evidence_logs: list[str]) -> None:
    raw = path.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw else "\n"
    data = json.loads(raw)
    t = result["recommended_t_dead_s"]
    data["live"]["source_dead_timeout_seconds"] = t
    data.setdefault("live_results", {})["T_dead"] = {
        "value_seconds": t,
        "formula": "max(3.0, round_up(2 x observed_max_gap + 1.0, 0.5))",
        "observed_max_gap_s": result["observed_max_gap_s"],
        "max_gap_by_kind_s": result["max_gap_by_kind_s"],
        "gap_basis": "conservative max over per-stream max_gap_s (audio/video separately)",
        "runs": result["runs"],
        "minutes_watched": round(result["minutes"], 1),
        "logs": evidence_logs,
    }
    out = json.dumps(data, indent=2, ensure_ascii=False).replace("\n", nl)
    if raw.endswith(("\n", "\r\n")):
        out += nl
    path.write_bytes(out.encode("utf-8"))


_YAML_LINE = re.compile(r"^(\s+source_dead_timeout_seconds:\s*)([^\s#]+)(.*)$")


def write_config(path: Path, value: float) -> None:
    raw = path.read_bytes().decode("utf-8")
    lines = raw.splitlines(keepends=True)
    in_liveness = False
    for i, line in enumerate(lines):
        stripped = line.rstrip("\r\n")
        if re.match(r"^\S", stripped) and not stripped.startswith("#"):
            in_liveness = stripped.startswith("liveness:")
            continue
        m = _YAML_LINE.match(stripped) if in_liveness else None
        if m:
            eol = line[len(stripped):]
            lines[i] = f"{m.group(1)}{value:g}{m.group(3)}{eol}"
            path.write_bytes("".join(lines).encode("utf-8"))
            return
    raise SystemExit(f"error: liveness.source_dead_timeout_seconds not found in {path}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs", nargs="+", type=Path)
    ap.add_argument("--min-minutes", type=float, default=10.0)
    ap.add_argument("--force", action="store_true", help="accept less than --min-minutes of evidence")
    ap.add_argument("--write", action="store_true", help="update the manifest and --config")
    ap.add_argument("--config", type=Path, help="config YAML to update with --write")
    ap.add_argument("--manifest", type=Path, default=MANIFEST)
    args = ap.parse_args(argv)

    if args.write and args.config is None:
        ap.error("--write requires --config")
    runs = []
    for p in args.logs:
        r = analyse_log(p)
        if r is None:
            print(f"skip {p}: no stats events with packets")
        else:
            runs.append(r)
            print(f"run {p}: {r['seconds'] / 60:.1f} min, max gaps "
                  + ", ".join(f"{k}={v:.3f}s" for k, v in sorted(r["max_gaps"].items())))
    result = aggregate(runs)
    print(f"runs={result['runs']} minutes_watched={result['minutes']:.1f}")
    print(f"max gap by kind (s): {result['max_gap_by_kind_s']}")
    print("note: per-stream gaps only; observed gap = max over streams (conservative)")
    print(f"observed_max_gap_s={result['observed_max_gap_s']}")
    print(f"recommended source_dead_timeout_seconds={result['recommended_t_dead_s']}")

    if not runs or result["minutes"] < args.min_minutes:
        msg = f"only {result['minutes']:.1f} min of watched source, need >= {args.min_minutes:g}"
        if not args.force:
            print(f"refused: {msg} (use --force to override)", file=sys.stderr)
            return 2
        print(f"warning: {msg} (forced)", file=sys.stderr)
        if not runs:
            return 2
    if args.write:
        write_manifest(args.manifest, result, [r["path"] for r in runs])
        write_config(args.config, result["recommended_t_dead_s"])
        print(f"wrote {args.manifest} and {args.config}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

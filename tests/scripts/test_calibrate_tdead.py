import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("calibrate_tdead", ROOT / "scripts" / "calibrate_tdead.py")
cal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cal)


def _log(path, minutes, audio_gap, video_gap):
    n = int(minutes * 12)
    lines = ["garbage line", json.dumps({"t": 1000.0, "event": "start"})]
    for i in range(n + 1):
        frac = i / n
        lines.append(json.dumps({"t": 1000.0 + i * 5, "event": "stats", "streams": {
            "1/audio": {"packets": 10 + i, "max_gap_s": audio_gap * frac},
            "2/video": {"packets": 10 + i, "max_gap_s": video_gap * frac},
        }}))
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def test_recommend_math():
    assert cal.recommend(0.0) == 3.0
    assert cal.recommend(1.0) == 3.0
    assert cal.recommend(1.2) == 3.5
    assert cal.recommend(2.0) == 5.0
    assert cal.recommend(2.3) == 6.0


def test_analyse_uses_final_cumulative_gap_and_duration(tmp_path):
    r = cal.analyse_log(_log(tmp_path / "a.jsonl", 10, 0.05, 1.2))
    assert r["seconds"] == pytest.approx(600)
    assert r["max_gaps"]["2/video"] == pytest.approx(1.2)


def test_refuses_short_run_unless_forced(tmp_path):
    p = _log(tmp_path / "a.jsonl", 5, 0.1, 0.2)
    assert cal.main([str(p)]) == 2
    assert cal.main([str(p), "--force"]) == 0


def test_two_runs_add_up(tmp_path, capsys):
    a = _log(tmp_path / "a.jsonl", 6, 0.1, 0.2)
    b = _log(tmp_path / "b.txt", 6, 0.1, 0.9)
    assert cal.main([str(a), str(b)]) == 0
    assert "recommended source_dead_timeout_seconds=3.0" in capsys.readouterr().out


def test_write_edits_only_target_lines(tmp_path):
    log = _log(tmp_path / "a.jsonl", 10, 0.1, 1.2)
    manifest_src = (ROOT / "manifests" / "verified-capabilities.json").read_bytes()
    manifest = tmp_path / "m.json"
    manifest.write_bytes(manifest_src)
    cfg_src = (ROOT / "config.example.yaml").read_bytes()
    cfg = tmp_path / "c.yaml"
    cfg.write_bytes(cfg_src)
    assert cal.main([str(log), "--write", "--config", str(cfg), "--manifest", str(manifest)]) == 0

    new_cfg = cfg.read_bytes().decode().splitlines()
    old_cfg = cfg_src.decode().splitlines()
    diff = [(o, n) for o, n in zip(old_cfg, new_cfg) if o != n]
    assert len(new_cfg) == len(old_cfg) and len(diff) == 1
    assert diff[0][1].strip() == "source_dead_timeout_seconds: 3.5   # must be the value measured in M0 before enabling"

    old = json.loads(manifest_src)
    new = json.loads(manifest.read_bytes())
    assert new["live"]["source_dead_timeout_seconds"] == 3.5
    assert new["live_results"]["T_dead"]["observed_max_gap_s"] == pytest.approx(1.2)
    new["live"]["source_dead_timeout_seconds"] = None
    del new["live_results"]["T_dead"]
    assert new == old

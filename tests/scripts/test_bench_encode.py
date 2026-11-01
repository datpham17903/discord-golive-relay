import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("bench_encode", ROOT / "scripts" / "bench_encode.py")
bench_encode = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_encode)


def test_bench_smoke():
    r = bench_encode.bench("saver", 0.2)
    assert r["frames"] == 6

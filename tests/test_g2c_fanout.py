import queue
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spikes" / "m0"))

from g2c_multi import OUTPUT_QUEUE_MAX, Fanout  # noqa: E402


def _item(n: int, idr: bool = False) -> dict:
    return {"n": n, "idr": idr}


def _drain(q: queue.Queue) -> list[int]:
    out = []
    while not q.empty():
        out.append(q.get_nowait()["n"])
    return out


def test_every_au_reaches_every_output_in_order():
    fan = Fanout(queue.Queue())
    outs = [fan.attach(str(i)) for i in range(3)]
    for n in range(OUTPUT_QUEUE_MAX):
        fan.dispatch(_item(n, idr=n == 0))
    assert all(_drain(o.q) == list(range(OUTPUT_QUEUE_MAX)) for o in outs)


def test_thread_moves_items_from_source():
    src: queue.Queue = queue.Queue()
    fan = Fanout(src)
    a, b = fan.attach("a"), fan.attach("b")
    fan.start()
    for n in range(5):
        src.put(_item(n, idr=n == 0))
    deadline = time.monotonic() + 2
    while (a.delivered < 5 or b.delivered < 5) and time.monotonic() < deadline:
        time.sleep(0.01)
    fan.stop()
    assert a.delivered == b.delivered == 5


def test_late_joiner_starts_at_idr_and_detach_purges():
    fan = Fanout(queue.Queue())
    early = fan.attach("early")
    fan.dispatch(_item(0, idr=True))
    late = fan.attach("late")
    fan.dispatch(_item(1))
    fan.dispatch(_item(2, idr=True))
    fan.dispatch(_item(3))
    assert _drain(early.q) == [0, 1, 2, 3]
    assert _drain(late.q) == [2, 3]
    fan.detach(early)
    fan.dispatch(_item(4))
    assert early.q.empty() and late.q.qsize() == 1


def test_full_output_drops_and_resyncs_without_affecting_others():
    fan = Fanout(queue.Queue())
    slow, fast = fan.attach("slow"), fan.attach("fast")
    for n in range(OUTPUT_QUEUE_MAX + 2):
        fan.dispatch(_item(n, idr=n == 0))
        _drain(fast.q)
    assert slow.dropped == 1 and slow.waiting_idr
    assert fast.dropped == 0


def test_detach_during_dispatch_leaves_nothing_in_purged_queue():
    import threading

    fan = Fanout(queue.Queue())
    out = fan.attach("a")
    stop = threading.Event()

    def spam():
        n = 0
        while not stop.is_set():
            fan.dispatch(_item(n, idr=True))
            n += 1

    t = threading.Thread(target=spam)
    t.start()
    time.sleep(0.01)
    fan.detach(out)
    stop.set()
    t.join()
    assert out.q.empty()

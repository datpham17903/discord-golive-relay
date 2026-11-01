from relay.supervisor.liveness import LivenessTracker
from relay.supervisor.model import Liveness


def test_unknown_before_progress():
    t = LivenessTracker(10.0)
    assert t.state(100.0) is Liveness.UNKNOWN
    assert t.age(100.0) is None


def test_live_until_t_dead_then_dead():
    t = LivenessTracker(10.0)
    t.progress(100.0)
    assert t.state(100.0) is Liveness.LIVE
    assert t.state(110.0) is Liveness.LIVE
    assert t.state(110.01) is Liveness.DEAD
    assert t.age(105.0) == 5.0


def test_progress_never_goes_backwards():
    t = LivenessTracker(10.0)
    t.progress(100.0)
    t.progress(90.0)
    assert t.age(100.0) == 0.0


def test_reset():
    t = LivenessTracker(10.0)
    t.progress(100.0)
    t.reset()
    assert t.state(101.0) is Liveness.UNKNOWN

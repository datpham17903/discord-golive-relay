import random

import pytest

from golive_relay.supervisor.retry import RetryPolicy

DELAYS = [1, 2, 4, 8, 15]


def test_sequence_within_jitter_then_cooldown():
    p = RetryPolicy(DELAYS, 60.0, rng=random.Random(1))
    for base in DELAYS:
        d = p.next_delay()
        assert base * 0.8 <= d <= base * 1.2
    assert p.next_delay() == 60.0


def test_restarts_after_cooldown():
    p = RetryPolicy(DELAYS, 60.0, rng=random.Random(1))
    for _ in range(6):
        p.next_delay()
    assert 0.8 <= p.next_delay() <= 1.2


def test_reset():
    p = RetryPolicy(DELAYS, 60.0, rng=random.Random(1))
    p.next_delay()
    p.next_delay()
    p.reset()
    assert 0.8 <= p.next_delay() <= 1.2


def test_zero_jitter_exact():
    p = RetryPolicy(DELAYS, 60.0, jitter=0.0)
    assert [p.next_delay() for _ in range(6)] == [1, 2, 4, 8, 15, 60]


def test_short_delays_list_repeats_last():
    p = RetryPolicy([1, 2], 60.0, max_failures=4, jitter=0.0)
    assert [p.next_delay() for _ in range(5)] == [1, 2, 2, 2, 60]


@pytest.mark.parametrize("seed", range(20))
def test_jitter_bounds(seed):
    p = RetryPolicy([10], 60.0, max_failures=1, rng=random.Random(seed))
    assert 8.0 <= p.next_delay() <= 12.0

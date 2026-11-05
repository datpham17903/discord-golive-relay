from golive_relay.supervisor.scaling import RelayScaler, relays_needed


def test_formula_examples():
    assert [relays_needed(n, 50, True) for n in (99, 100, 150, 200)] == [0, 1, 2, 3]
    assert [relays_needed(n, 50, False) for n in (49, 50, 99, 100)] == [0, 1, 1, 2]


def test_scale_up_immediate():
    s = RelayScaler(120)
    assert s.update(0, 0) == 0
    assert s.update(2, 1) == 2


def test_scale_down_after_delay_only():
    s = RelayScaler(120)
    s.update(2, 0)
    assert s.update(0, 10) == 2
    assert s.update(0, 129) == 2
    assert s.update(0, 130) == 0


def test_flapping_around_threshold_never_scales_down_early():
    s = RelayScaler(120)
    s.update(2, 0)
    for t in range(10, 600, 100):
        assert s.update(1, t) == 2
        assert s.update(2, t + 50) == 2


def test_scale_down_to_window_max():
    s = RelayScaler(120)
    s.update(3, 0)
    s.update(0, 10)
    s.update(2, 60)
    s.update(1, 100)
    assert s.update(0, 130) == 2

from golive_relay.supervisor.scaling import relays_needed


def test_formula_examples():
    assert [relays_needed(n, 50, True) for n in (99, 100, 150, 200)] == [0, 1, 2, 3]
    assert [relays_needed(n, 50, False) for n in (49, 50, 99, 100)] == [0, 1, 1, 2]


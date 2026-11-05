from golive_relay.control.cooldown import ChatCooldowns


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_author_cooldown_notifies_once_then_silent():
    clk = Clock()
    cd = ChatCooldowns(5, 3, clk)
    assert cd.author(1) is None
    clk.t += 1.2
    assert cd.author(1) == 4  # one notice with the seconds left
    clk.t += 1
    assert cd.author(1) == 0  # silent for the rest of the window
    assert cd.author(2) is None  # other authors are independent
    clk.t += 3
    assert cd.author(1) is None  # window over: allowed and a new window starts
    assert cd.author(1) == 5


def test_author_entries_are_pruned_after_the_cooldown():
    clk = Clock()
    cd = ChatCooldowns(5, 3, clk)
    for a in range(100):
        cd.author(a)
    clk.t += 5
    cd.author(1000)
    assert len(cd._authors) == 1


def test_change_cooldown_is_global():
    clk = Clock()
    cd = ChatCooldowns(5, 3, clk)
    assert cd.change_wait() == 0
    cd.changed()
    clk.t += 0.5
    assert cd.change_wait() == 3
    clk.t += 2
    assert cd.change_wait() == 1
    clk.t += 0.5
    assert cd.change_wait() == 0


def test_zero_disables_cooldowns():
    cd = ChatCooldowns(0, 0, Clock())
    assert cd.author(1) is None and cd.author(1) is None
    cd.changed()
    assert cd.change_wait() == 0

from relay.supervisor.model import RelayVoiceObserved
from relay.supervisor.ownership import OwnershipDetector, OwnershipVerdict

CHAN = 20


def obs(session, channel):
    return RelayVoiceObserved(session_id=session, channel_id=channel)


def armed() -> OwnershipDetector:
    d = OwnershipDetector()
    d.arm("s1")
    return d


def test_ok():
    assert armed().classify(obs("s1", CHAN), CHAN, 0.0) is OwnershipVerdict.OK


def test_same_session_null_channel_displaced():
    assert armed().classify(obs("s1", None), CHAN, 0.0) is OwnershipVerdict.DISPLACED


def test_same_session_other_channel_displaced():
    assert armed().classify(obs("s1", 99), CHAN, 0.0) is OwnershipVerdict.DISPLACED


def test_other_session_uncertain():
    assert armed().classify(obs("s2", CHAN), CHAN, 0.0) is OwnershipVerdict.OWNERSHIP_UNCERTAIN


def test_missing_session_missing_evidence():
    assert armed().classify(obs(None, CHAN), CHAN, 0.0) is OwnershipVerdict.MISSING_EVIDENCE


def test_missing_baseline_missing_evidence():
    d = OwnershipDetector()
    d.arm(None)
    assert d.classify(obs("s1", CHAN), CHAN, 0.0) is OwnershipVerdict.MISSING_EVIDENCE


def test_disarmed_ignored():
    d = armed()
    d.disarm()
    assert d.classify(obs("s2", None), CHAN, 0.0) is OwnershipVerdict.IGNORED


def test_never_armed_ignored():
    assert OwnershipDetector().classify(obs("s2", None), CHAN, 0.0) is OwnershipVerdict.IGNORED


def test_exempt_window():
    d = armed()
    d.exempt(5.0, now=100.0)
    assert d.classify(obs("s2", None), CHAN, 104.9) is OwnershipVerdict.IGNORED
    assert d.classify(obs("s2", None), CHAN, 105.0) is OwnershipVerdict.OWNERSHIP_UNCERTAIN

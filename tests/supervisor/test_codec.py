from golive_relay.media.bitstream import down_from
from golive_relay.supervisor.codec import (
    ActionFailed, CodecController, Hold, Live, OutputAdded, OutputBound, OutputCodec, OutputGone,
    SourceCodec, SwapOutput, Tick, UpdateOutput, UpdateWatch,
)


def settle_h264(c):
    c.handle(SourceCodec("AV1", True), 0)
    c.handle(OutputAdded("a", "AV1", "AV1"), 0)
    c.handle(OutputAdded("b", "H264", "AV1"), 0)
    c.handle(SourceCodec("H264", True), 1)
    c.handle(OutputCodec("a", "H264"), 1.1)


def test_initial_outputs_pinned_to_source_and_swapped():
    c = CodecController(pinned=False)
    assert c.handle(SourceCodec("H264", True), 0.0) == []
    acts = c.handle(OutputAdded("a", "AV1", "AV1"), 0.0)
    assert acts == [UpdateOutput("a", down_from("H264"))]
    assert c.handle(OutputCodec("a", "H264"), 0.1) == [SwapOutput("a", "H264")]
    assert c.effective == "H264"


def test_divergent_outputs_converge():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("AV1", True), 0)
    assert c.handle(OutputAdded("a", "AV1", "AV1"), 0) == [SwapOutput("a", "AV1")]
    acts = c.handle(OutputAdded("b", "H264", "AV1"), 0)
    assert acts == [UpdateWatch(down_from("H264"))]
    assert c.switching == ("AV1", "H264")
    acts = c.handle(SourceCodec("H264", True), 1)
    assert acts == [UpdateOutput("a", down_from("H264")), SwapOutput("b", "H264")]
    assert c.handle(OutputCodec("a", "H264"), 1.1) == [SwapOutput("a", "H264")]
    assert c.switching is None


def test_weak_viewer_leaves_upgrades():
    c = CodecController(pinned=False)
    settle_h264(c)
    acts = c.handle(OutputCodec("b", "AV1"), 10)
    assert acts == [UpdateWatch(down_from("AV1"))]
    assert c.switching == ("H264", "AV1")
    acts = c.handle(SourceCodec("AV1", True), 10.5)
    assert UpdateOutput("a", down_from("AV1")) in acts
    assert c.handle(OutputCodec("a", "AV1"), 10.6) == [SwapOutput("a", "AV1")]
    assert c.effective == "AV1"
    assert c.switching is None


def test_partial_upgrade_settles():
    c = CodecController(pinned=False)
    settle_h264(c)
    c.handle(OutputCodec("b", "AV1"), 10)
    c.handle(SourceCodec("AV1", True), 10.5)
    # b only follows to H265 after the raised offer
    acts = c.handle(OutputCodec("b", "H265"), 10.6)
    acts += c.handle(OutputCodec("a", "H265"), 10.6)
    acts += c.handle(SourceCodec("H265", True), 11)
    assert UpdateWatch(down_from("H265")) in acts
    assert [x for x in acts if isinstance(x, SwapOutput) and x.output_id == "b"] == [SwapOutput("b", "H265")]
    assert c.effective == "H265"
    acts = []
    for i in range(10):
        acts += c.handle(Tick(), 12 + i)
    assert acts == []
    assert c.effective == "H265"


def test_target_side_drop_and_recovery():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("AV1", True), 0)
    c.handle(OutputAdded("a", "AV1", "AV1"), 0)
    acts = c.handle(SourceCodec("H264", True), 5)
    assert acts == [UpdateOutput("a", down_from("H264"))]
    assert c.handle(OutputCodec("a", "H264"), 5.1) == [SwapOutput("a", "H264")]
    acts = c.handle(SourceCodec("AV1", True), 9)
    assert acts == [UpdateOutput("a", down_from("AV1"))]
    assert c.handle(OutputCodec("a", "AV1"), 9.1) == [SwapOutput("a", "AV1")]


def test_upgrade_wait_expires_pins_outputs():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H264", True), 0)
    c.handle(OutputAdded("a", "H264", "AV1"), 0)
    c.handle(OutputAdded("b", "AV1", "AV1"), 0)
    c.handle(OutputCodec("a", "AV1"), 10)  # viewer left: watch widened at 10
    assert c.handle(Tick(), 11) == []
    acts = c.handle(Tick(), 13.5)
    assert UpdateOutput("a", down_from("H264")) in acts


def test_swap_only_when_reported_equals_e():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H265", True), 0)
    acts = c.handle(OutputAdded("a", "H264", "AV1"), 0)
    assert not any(isinstance(x, SwapOutput) for x in acts)
    acts = c.handle(OutputCodec("a", "H265"), 1)
    assert SwapOutput("a", "H265") in acts


def test_short_hold_no_renegotiation():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H265", True), 0)
    c.handle(OutputAdded("a", "H265", "H265"), 0)
    assert c.handle(Hold(), 1) == []
    assert c.handle(Tick(), 2.5) == []
    assert c.handle(Live(), 2.6) == []
    assert c.effective == "H265"


def test_hold_then_live_restores():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H265", True), 0)
    c.handle(OutputAdded("a", "H265", "H265"), 0)
    c.handle(Hold(), 1)
    acts = c.handle(Tick(), 3.5)
    assert acts == [UpdateOutput("a", down_from("H264"))]
    assert c.effective == "H264"
    assert c.handle(OutputCodec("a", "H264"), 3.6) == [SwapOutput("a", "H264")]
    acts = c.handle(Live(), 5)
    assert c.effective == "H265"
    assert UpdateOutput("a", down_from("H265")) in acts
    assert c.handle(OutputCodec("a", "H265"), 5.1) == [SwapOutput("a", "H265")]


def test_ineligible_source_forces_h264():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("AV1", False), 0)
    assert c.effective == "H264"
    acts = c.handle(OutputAdded("a", "AV1", "AV1"), 0)
    assert acts == [UpdateOutput("a", down_from("H264"))]


def test_pinned_mode_only_h264():
    c = CodecController(pinned=True)
    assert c.handle(SourceCodec("AV1", True), 0) == []
    assert c.effective == "H264"
    acts = c.handle(OutputAdded("a", "AV1", "AV1"), 0)
    assert acts == [UpdateOutput("a", ("H264",))]
    assert c.handle(OutputCodec("a", "H264"), 0.1) == [SwapOutput("a", "H264")]
    assert not any(isinstance(x, UpdateWatch) for x in c.handle(Tick(), 5))


def test_output_gone_recomputes_watch():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H264", True), 0)
    acts = c.handle(OutputAdded("b", "H264", "AV1"), 0)
    assert UpdateWatch(down_from("H264")) in acts
    assert c.handle(OutputGone("b"), 1) == [UpdateWatch(down_from("AV1"))]


def test_vp8_natural_viewer_drops_below_h264():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H264", True), 0)
    acts = c.handle(OutputAdded("v", "VP8", "AV1"), 0)
    assert acts == [UpdateWatch(down_from("VP8"))]
    assert c.handle(SourceCodec("VP8", True), 1) == [SwapOutput("v", "VP8")]
    assert c.effective == "VP8"


def test_idempotent_no_duplicate_actions():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H264", True), 0)
    c.handle(OutputAdded("a", "AV1", "AV1"), 0)
    c.handle(OutputAdded("b", "VP8", "AV1"), 0)
    for t in range(1, 8):
        assert c.handle(Tick(), t) == []
    assert c.handle(SourceCodec("H264", True), 9) == []


def test_swap_bound_elsewhere_is_reemitted():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H265", True), 0)
    c.handle(OutputAdded("a", "H264", "AV1"), 0)
    assert SwapOutput("a", "H265") in c.handle(OutputCodec("a", "H265"), 1)
    # op 14 moved the output to H264 between emission and execution of the swap
    c.handle(OutputCodec("a", "H264"), 1.4)
    assert c.handle(OutputBound("a", "H264"), 1.5) == []
    assert SwapOutput("a", "H265") in c.handle(OutputCodec("a", "H265"), 2)
    assert c.handle(OutputBound("a", None), 2.5) == []  # failed swap: no immediate retry
    assert c.handle(Tick(), 3) == []  # retry cooldown
    assert c.handle(Tick(), 7.6) == [SwapOutput("a", "H265")]


def test_action_failed_reemits_on_next_reconcile():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H264", True), 0)
    w, _swap = c.handle(OutputAdded("b", "H264", "AV1"), 0)
    assert isinstance(w, UpdateWatch)
    assert c.handle(ActionFailed(w), 0.5) == []
    assert c.handle(Tick(), 1) == [w]
    u = UpdateOutput("a", down_from("H264"))
    assert c.handle(OutputAdded("a", "AV1", "AV1"), 1) == [u]
    c.handle(ActionFailed(u), 1.1)
    assert c.handle(Tick(), 1.2) == [u]
    s = SwapOutput("a", "H264")
    assert c.handle(OutputCodec("a", "H264"), 1.3) == [s]
    c.handle(ActionFailed(s), 1.4)
    assert c.handle(Tick(), 1.5) == []  # retry cooldown
    assert c.handle(Tick(), 6.5) == [s]


def test_failed_swap_retried_after_cooldown():
    c = CodecController(pinned=False)
    c.handle(SourceCodec("H265", True), 0.0)
    acts = c.handle(OutputAdded("a", "H265", "AV1"), 0.0)
    assert SwapOutput("a", "H265") in acts
    c.handle(OutputBound("a", None), 0.1)
    assert not any(isinstance(x, SwapOutput) for x in c.handle(Tick(), 1.0))
    assert SwapOutput("a", "H265") in c.handle(Tick(), 5.2)

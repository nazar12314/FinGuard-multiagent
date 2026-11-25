import pytest

from finguard.core.diagnostics import OutboxState, PingState, ping_state


@pytest.mark.parametrize(
    ("outbox_state", "handled", "failures", "expected"),
    [
        ("queued", False, [], "queued"),
        ("parked", False, [], "parked"),
        ("published", False, [], "published"),
        ("published", False, [False], "failing"),
        ("published", False, [False] * 9, "failing"),
        ("published", False, [False, True], "dead_lettered"),
        ("published", True, [False] * 4, "handled"),
    ],
)
def test_a_pings_state_is_what_its_records_show(
    outbox_state: OutboxState, handled: bool, failures: list[bool], expected: PingState
) -> None:
    assert ping_state(outbox_state=outbox_state, handled=handled, failures=failures) == expected

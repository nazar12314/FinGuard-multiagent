import pytest

from finguard.core.diagnostics import OutboxState, PingState, describe_progress

SOURCE = "diagnostics.pings"


@pytest.mark.parametrize(
    ("outbox_state", "handled", "failures", "expected"),
    [
        ("queued", False, [], ("queued", None)),
        ("parked", False, [], ("parked", None)),
        ("published", False, [], ("published", SOURCE)),
        ("published", False, [False], ("retrying", SOURCE)),
        ("published", False, [False] * 3, ("retrying", f"{SOURCE}.retry.1")),
        ("published", False, [False] * 7, ("retrying", f"{SOURCE}.retry.2")),
        ("published", False, [False] * 9, ("dead_lettered", f"{SOURCE}.dlq")),
        ("published", False, [True], ("dead_lettered", f"{SOURCE}.dlq")),
        ("published", True, [False] * 4, ("handled", None)),
    ],
)
def test_a_pings_position_follows_the_retry_policy(
    outbox_state: OutboxState,
    handled: bool,
    failures: list[bool],
    expected: tuple[PingState, str | None],
) -> None:
    # Three attempts per topic, two retry tiers: nine failed attempts before the DLQ.
    progress = describe_progress(
        outbox_state=outbox_state,
        handled=handled,
        failures=failures,
        attempts_per_topic=3,
        retry_tiers=2,
    )

    assert progress == expected

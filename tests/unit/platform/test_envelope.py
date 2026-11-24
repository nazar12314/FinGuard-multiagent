from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import JsonValue, ValidationError

from finguard.platform.envelope import Envelope
from finguard.platform.ids import new_id

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

json_values: st.SearchStrategy[JsonValue] = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**63), max_value=2**63 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)

envelopes = st.builds(
    Envelope,
    id=st.uuids(),
    type=st.from_regex(r"\A[a-z][a-z0-9_]{0,10}(\.[a-z][a-z0-9_]{0,10}){1,3}\Z"),
    version=st.integers(min_value=1, max_value=100),
    occurred_at=st.datetimes(timezones=st.just(UTC)),
    correlation_id=st.uuids(),
    causation_id=st.none() | st.uuids(),
    traceparent=st.none() | st.just(TRACEPARENT),
    data=st.dictionaries(st.text(), json_values, max_size=5),
)


def test_new_message_starts_its_own_correlation_chain() -> None:
    envelope = Envelope.new("cases.case.opened", {"case_id": "c-1"})

    assert envelope.correlation_id == envelope.id
    assert envelope.causation_id is None
    assert envelope.version == 1
    assert envelope.occurred_at.tzinfo is UTC


def test_new_message_joins_a_given_correlation_chain() -> None:
    case_id = new_id()

    envelope = Envelope.new("agent.task.requested", {}, correlation_id=case_id)

    assert envelope.correlation_id == case_id
    assert envelope.id != case_id


def test_follow_up_keeps_correlation_and_points_at_its_cause() -> None:
    parent = Envelope.new("alerts.alert.created", {"alert_id": "a-1"})

    child = parent.follow_up("cases.case.opened", {"case_id": "c-1"}, version=2)

    assert child.id != parent.id
    assert child.correlation_id == parent.correlation_id
    assert child.causation_id == parent.id
    assert child.type == "cases.case.opened"
    assert child.version == 2


def test_occurred_at_is_normalised_to_utc() -> None:
    kyiv_noon = datetime(2026, 11, 24, 12, 0, tzinfo=timezone(timedelta(hours=2)))

    envelope = Envelope(
        type="tx.raw.received", occurred_at=kyiv_noon, correlation_id=new_id(), data={}
    )

    assert envelope.occurred_at == datetime(2026, 11, 24, 10, 0, tzinfo=UTC)
    assert envelope.model_dump(mode="json")["occurred_at"] == "2026-11-24T10:00:00Z"


@pytest.mark.parametrize(
    "override",
    [
        {"occurred_at": datetime(2026, 11, 24, 12, 0)},
        {"type": "NotDotted"},
        {"type": "cases."},
        {"version": 0},
        {"traceparent": "not-a-traceparent"},
        {"unexpected_field": 1},
    ],
    ids=[
        "naive-datetime",
        "undotted-type",
        "empty-segment",
        "zero-version",
        "bad-traceparent",
        "extra-field",
    ],
)
def test_rejects_invalid_envelopes(override: dict[str, Any]) -> None:
    fields: dict[str, Any] = {"type": "cases.case.opened", "correlation_id": new_id(), "data": {}}

    with pytest.raises(ValidationError):
        Envelope(**(fields | override))


def test_envelope_is_immutable() -> None:
    envelope = Envelope.new("cases.case.opened", {})

    with pytest.raises(ValidationError):
        envelope.type = "cases.case.closed"


def test_wire_format_uses_the_documented_field_names() -> None:
    envelope = Envelope.new("cases.case.opened", {"case_id": "c-1"})

    assert set(envelope.model_dump(mode="json")) == {
        "id",
        "type",
        "version",
        "occurred_at",
        "correlation_id",
        "causation_id",
        "traceparent",
        "data",
    }


@given(envelopes)
def test_json_round_trip_preserves_the_envelope(envelope: Envelope) -> None:
    assert Envelope.model_validate_json(envelope.model_dump_json()) == envelope

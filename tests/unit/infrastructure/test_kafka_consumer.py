import pytest
from pydantic import BaseModel, ValidationError

from finguard.infrastructure.kafka.consumer import RetryPolicy, is_permanent
from finguard.platform.errors import (
    Conflict,
    DependencyUnavailable,
    NotFound,
    ValidationFailed,
    describe_error,
)


class Amount(BaseModel):
    value: int


def validation_error() -> ValidationError:
    try:
        Amount.model_validate({"value": "many"})
    except ValidationError as error:
        return error
    raise AssertionError("expected a validation error")


@pytest.mark.parametrize(
    ("error", "permanent"),
    [
        (ValidationFailed("closing needs a rationale"), True),
        (Conflict("decision already recorded"), True),
        (NotFound("Case", 42), True),
        (validation_error(), True),
        (DependencyUnavailable("database unavailable"), False),
        (ConnectionError("connection reset"), False),
        (TimeoutError(), False),
    ],
)
def test_only_errors_a_retry_cannot_change_are_permanent(error: Exception, permanent: bool) -> None:
    assert is_permanent(error) is permanent


def test_the_longest_handling_time_covers_every_attempt_and_backoff() -> None:
    policy = RetryPolicy(
        attempts=3, attempt_backoff_s=0.5, handler_timeout_s=60, retry_delays_s=(30.0,)
    )

    assert policy.longest_handling_s == 3 * 60 + 0.5 + 1.0


def test_error_descriptions_never_quote_message_data() -> None:
    assert describe_error(ValidationFailed("closing needs a rationale")) == (
        "ValidationFailed: closing needs a rationale"
    )
    assert describe_error(validation_error()) == "ValidationError: value: int_parsing"
    assert describe_error(ConnectionError("Key (iban)=(DE89 3704 0044) already exists")) == (
        "ConnectionError"
    )

import json
from collections.abc import Iterator
from typing import Literal
from uuid import uuid4

import pytest
from pydantic import BaseModel, ConfigDict, Field
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from finguard.infrastructure.schema_registry import SchemaRegistry
from finguard.platform.contracts import MessageContract, message_schema

# Same image as deploy/compose/compose.yaml.
APICURIO_IMAGE = "apicurio/apicurio-registry:3.3.5"


@pytest.fixture(scope="module")
def registry() -> Iterator[SchemaRegistry]:
    container = (
        DockerContainer(APICURIO_IMAGE)
        .with_exposed_ports(8080)
        .waiting_for(LogMessageWaitStrategy("Listening on").with_startup_timeout(120))
    )
    with container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(8080)
        yield SchemaRegistry(f"http://{host}:{port}/apis/ccompat/v7")


def schema(properties: dict[str, object], required: list[str], *, closed: bool = True) -> str:
    # Message data models forbid extra fields, which their schemas state as below.
    return json.dumps(
        {
            "type": "object",
            "properties": properties,
            "required": required,
            **({"additionalProperties": False} if closed else {}),
        }
    )


V1 = schema({"case_id": {"type": "string"}}, ["case_id"])


@pytest.fixture
def subject() -> str:
    return f"test.case.opened.{uuid4().hex[:8]}.v1"


def test_registering_an_unchanged_schema_again_is_harmless(
    registry: SchemaRegistry, subject: str
) -> None:
    assert registry.register(subject, V1) == registry.register(subject, V1)


def test_a_schema_for_a_new_subject_has_nothing_to_be_compatible_with(
    registry: SchemaRegistry, subject: str
) -> None:
    assert registry.is_compatible(subject, V1) is None


@pytest.mark.parametrize(
    ("change", "compatible"),
    [
        (schema({"case_id": {"type": "string"}, "note": {"type": "string"}}, ["case_id"]), True),
        (
            schema(
                {"case_id": {"type": "string"}, "reason": {"type": "string"}},
                ["case_id", "reason"],
            ),
            False,
        ),
        (schema({"case_id": {"type": "integer"}}, ["case_id"]), False),
    ],
    ids=["optional-field-added", "required-field-added", "field-retyped"],
)
def test_only_backward_compatible_changes_pass(
    registry: SchemaRegistry, subject: str, change: str, compatible: bool
) -> None:
    registry.register(subject, V1)

    assert registry.is_compatible(subject, change) is compatible


def test_an_open_schema_cannot_gain_a_field_compatibly(
    registry: SchemaRegistry, subject: str
) -> None:
    # Old data could already carry `note` with any type, so narrowing it may break old data.
    registry.register(subject, schema({"case_id": {"type": "string"}}, ["case_id"], closed=False))
    gains_note = schema(
        {"case_id": {"type": "string"}, "note": {"type": "string"}}, ["case_id"], closed=False
    )

    assert registry.is_compatible(subject, gains_note) is False


class Payment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_eur: float = Field(gt=0)
    channel: Literal["sepa"] = "sepa"


class CardPayment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_eur: float = Field(gt=0)
    channel: Literal["card"] = "card"


def contract_schema(subject: str, data: type[BaseModel]) -> str:
    message_type = subject.removesuffix(".v1")
    return json.dumps(message_schema(MessageContract(message_type, 1, data)))


def test_a_generated_schema_with_a_strict_bound_can_be_checked_again(
    registry: SchemaRegistry, subject: str
) -> None:
    registry.register(subject, contract_schema(subject, Payment))

    assert registry.is_compatible(subject, contract_schema(subject, Payment)) is True


def test_changing_a_fixed_value_is_a_breaking_change(
    registry: SchemaRegistry, subject: str
) -> None:
    registry.register(subject, contract_schema(subject, Payment))

    assert registry.is_compatible(subject, contract_schema(subject, CardPayment)) is False

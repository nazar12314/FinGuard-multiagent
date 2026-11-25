from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, create_model

from finguard.platform.envelope import Envelope


@dataclass(frozen=True, slots=True)
class MessageContract:
    """A breaking change to `data` needs a new version, which is a new contract."""

    message_type: str
    version: int
    data: type[BaseModel]

    @property
    def subject(self) -> str:
        return f"{self.message_type}.v{self.version}"


class _OnTheWire(Envelope):
    # Producers always send every envelope field and consumers rely on them; `data` fields
    # keep their own optionality, so adding an optional one stays compatible.
    model_config = ConfigDict(json_schema_serialization_defaults_required=True)


# Apicurio reads a schema with no draft as draft 4 and misjudges exclusive bounds and const.
JSON_SCHEMA_DRAFT = "http://json-schema.org/draft-07/schema#"

# Keywords whose values are data, not schemas.
_DATA_KEYWORDS = frozenset({"const", "default", "enum", "examples"})


def message_schema(contract: MessageContract) -> dict[str, Any]:
    """The JSON Schema of the whole message on the wire: the envelope with this `data`."""
    _check_comparable(contract)
    message = create_model(
        contract.data.__name__ + "Message",
        __base__=_OnTheWire,
        type=(cast(Any, Literal)[contract.message_type], ...),
        version=(cast(Any, Literal)[contract.version], contract.version),
        data=(contract.data, ...),
    )
    schema = message.model_json_schema(mode="serialization")
    schema["title"] = contract.subject
    return {"$schema": JSON_SCHEMA_DRAFT, **schema}


def _check_comparable(contract: MessageContract) -> None:
    for schema in _subschemas(contract.data.model_json_schema(mode="serialization")):
        # An open object can't gain a field compatibly: old data may hold it with any type.
        if "properties" in schema and schema.get("additionalProperties") is not False:
            raise ValueError(
                f"{contract.subject}: {schema.get('title', 'a nested model')} must set"
                " extra='forbid'"
            )
        # Draft 7 has no prefixItems, so a retyped tuple element would pass as compatible.
        if "prefixItems" in schema:
            raise ValueError(
                f"{contract.subject}: fixed-length tuples can't be checked for compatibility;"
                " use a list or a model"
            )


def _subschemas(schema: Any) -> Iterator[dict[str, Any]]:
    if not isinstance(schema, dict):
        return
    yield schema
    for keyword, value in schema.items():
        if keyword in _DATA_KEYWORDS:
            continue
        if keyword in ("properties", "$defs", "patternProperties"):
            for subschema in value.values():
                yield from _subschemas(subschema)
        elif isinstance(value, dict):
            yield from _subschemas(value)
        elif isinstance(value, list):
            for subschema in value:
                yield from _subschemas(subschema)

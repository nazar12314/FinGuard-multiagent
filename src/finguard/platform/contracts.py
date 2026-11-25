from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, create_model

from finguard.platform.envelope import Envelope


@dataclass(frozen=True, slots=True)
class MessageContract:
    """One message type at one version, described by the model of its `data`.

    A breaking change to `data` needs a new version, which is a new contract with its own
    schema subject; within a version only backward-compatible changes are allowed.
    """

    message_type: str
    version: int
    data: type[BaseModel]

    @property
    def subject(self) -> str:
        return f"{self.message_type}.v{self.version}"


class _OnTheWire(Envelope):
    # Producers always send every envelope field, defaults included, and consumers rely on
    # them (`id` for deduplication). The fields of `data` keep their own optionality, so
    # adding an optional field there stays a backward-compatible change.
    model_config = ConfigDict(json_schema_serialization_defaults_required=True)


def message_schema(contract: MessageContract) -> dict[str, Any]:
    """The JSON Schema of the whole message on the wire: the envelope with this `data`."""
    # Under the registry's rules an open object can't gain a field compatibly: old data might
    # already carry that name with any type. A closed model can add optional fields.
    if contract.data.model_config.get("extra") != "forbid":
        raise ValueError(f"{contract.subject}: the data model must set extra='forbid'")
    message = create_model(
        contract.data.__name__ + "Message",
        __base__=_OnTheWire,
        # The literals pin the envelope's type and version to this contract.
        type=(cast(Any, Literal)[contract.message_type], ...),
        version=(cast(Any, Literal)[contract.version], contract.version),
        data=(contract.data, ...),
    )
    schema = message.model_json_schema(mode="serialization")
    schema["title"] = contract.subject
    return schema

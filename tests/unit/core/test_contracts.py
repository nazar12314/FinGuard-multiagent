import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft7Validator
from pydantic import BaseModel, ConfigDict

from finguard.core.contracts import (
    CONTRACTS,
    read,
    removed_without_retiring,
    render,
    stale,
    write,
)
from finguard.core.diagnostics import PING_REQUESTED, PING_REQUESTED_CONTRACT, PingRequest
from finguard.core.settings import get_settings
from finguard.entrypoints import contracts as contracts_command
from finguard.entrypoints.contracts import main
from finguard.platform.contracts import MessageContract, message_schema
from finguard.platform.envelope import Envelope

REPOSITORY_CONTRACTS = Path(__file__).parents[3] / "contracts"


class Note(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str


class OpenNote(BaseModel):
    text: str


class Author(BaseModel):
    name: str


class NoteWithOpenAuthor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    author: Author


class NoteWithPosition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    position: tuple[int, int]


NOTE_CONTRACT = MessageContract(message_type="cases.note.added", version=2, data=Note)


def test_a_message_schema_pins_the_type_and_version_and_describes_its_data() -> None:
    schema = message_schema(NOTE_CONTRACT)

    assert schema["title"] == "cases.note.added.v2"
    assert schema["properties"]["type"]["const"] == "cases.note.added"
    assert schema["properties"]["version"]["const"] == 2
    assert schema["properties"]["data"] == {"$ref": "#/$defs/Note"}
    assert schema["$defs"]["Note"]["required"] == ["text"]
    # Producers always send every envelope field, so consumers may rely on them.
    assert set(schema["required"]) == {
        "id",
        "type",
        "version",
        "occurred_at",
        "correlation_id",
        "causation_id",
        "traceparent",
        "data",
    }


def test_a_message_schema_names_the_draft_the_registry_must_read_it_as() -> None:
    # Read as draft 4, exclusive bounds break the registry's checks and const is ignored.
    assert message_schema(NOTE_CONTRACT)["$schema"] == "http://json-schema.org/draft-07/schema#"


@pytest.mark.parametrize(
    ("data", "problem"),
    [
        (OpenNote, "OpenNote must set extra='forbid'"),
        (NoteWithOpenAuthor, "Author must set extra='forbid'"),
        (NoteWithPosition, "fixed-length tuples"),
    ],
    ids=["open-model", "open-nested-model", "tuple"],
)
def test_data_models_the_registry_cannot_judge_are_refused(
    data: type[BaseModel], problem: str
) -> None:
    contract = MessageContract(message_type="cases.note.added", version=3, data=data)

    with pytest.raises(ValueError, match=problem):
        message_schema(contract)


def committed_ping_schema() -> Draft7Validator:
    schema = json.loads((REPOSITORY_CONTRACTS / "diagnostics.ping.requested.v1.json").read_text())
    return Draft7Validator(schema)


def test_real_ping_messages_match_the_committed_schema() -> None:
    first = Envelope.new(PING_REQUESTED, PingRequest(fail="transient", times=3).model_dump())
    follow_up = first.follow_up(PING_REQUESTED, PingRequest().model_dump())

    for message in (first, follow_up):
        committed_ping_schema().validate(json.loads(message.model_dump_json()))


@pytest.mark.parametrize(
    "change",
    [
        {"type": "diagnostics.ping.other"},
        {"version": 2},
        {"data": {"fail": "never", "times": 1, "colour": "red"}},
        {"data": {"fail": "never", "times": 21}},
    ],
    ids=["wrong-type", "wrong-version", "unknown-data-field", "out-of-range"],
)
def test_the_committed_schema_rejects_messages_that_break_the_contract(
    change: dict[str, Any],
) -> None:
    message = json.loads(Envelope.new(PING_REQUESTED, PingRequest().model_dump()).model_dump_json())

    assert not committed_ping_schema().is_valid({**message, **change})


def test_the_committed_contracts_match_the_message_models() -> None:
    assert stale(REPOSITORY_CONTRACTS, CONTRACTS) == []


def test_generating_writes_missing_files_and_removes_files_without_a_contract(
    tmp_path: Path,
) -> None:
    (tmp_path / "cases.retired.v1.json").write_text("{}")
    (tmp_path / "README.md").write_text("kept")

    changed = write(tmp_path, [NOTE_CONTRACT])

    assert changed == ["cases.note.added.v2.json", "cases.retired.v1.json"]
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "README.md",
        "cases.note.added.v2.json",
    ]
    assert write(tmp_path, [NOTE_CONTRACT]) == []
    assert read(tmp_path) == {
        "cases.note.added.v2": render([NOTE_CONTRACT])["cases.note.added.v2.json"]
    }


def test_missing_outdated_and_uncontracted_files_are_reported(tmp_path: Path) -> None:
    write(tmp_path, [NOTE_CONTRACT, PING_REQUESTED_CONTRACT])
    outdated = tmp_path / "cases.note.added.v2.json"
    outdated.write_text(json.dumps({"title": "hand edited"}))

    assert stale(tmp_path, [NOTE_CONTRACT]) == [
        "cases.note.added.v2.json",
        "diagnostics.ping.requested.v1.json",
    ]


def test_the_check_command_fails_until_the_files_are_generated(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as failure:
        main(["--directory", str(tmp_path), "check"])
    assert failure.value.code == 1
    assert "finguard-contracts generate" in capsys.readouterr().out

    main(["--directory", str(tmp_path), "generate"])
    main(["--directory", str(tmp_path), "check"])


def test_a_contract_removed_since_the_base_must_be_declared_retired(tmp_path: Path) -> None:
    write(tmp_path, [NOTE_CONTRACT, PING_REQUESTED_CONTRACT])

    assert removed_without_retiring(tmp_path, [PING_REQUESTED_CONTRACT], frozenset()) == [
        "cases.note.added.v2"
    ]
    assert (
        removed_without_retiring(
            tmp_path, [PING_REQUESTED_CONTRACT], frozenset({"cases.note.added.v2"})
        )
        == []
    )


@pytest.fixture
def fresh_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[pytest.MonkeyPatch]:
    """Settings read from the environment this test sets, not cached from an earlier one."""
    monkeypatch.setenv("FINGUARD_ENV", "test")
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


class FakeRegistry:
    def __init__(self, url: str) -> None:
        self.url = url

    def is_compatible(self, subject: str, schema: str) -> bool | None:
        return "BREAKING" not in schema


def test_check_compatibility_fails_on_a_removed_or_breaking_contract(
    tmp_path: Path, fresh_settings: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fresh_settings.setattr(contracts_command, "SchemaRegistry", FakeRegistry)
    base, branch = tmp_path / "base", tmp_path / "branch"
    base.mkdir()
    branch.mkdir()
    write(base, [NOTE_CONTRACT, PING_REQUESTED_CONTRACT])
    write(branch, [PING_REQUESTED_CONTRACT])

    with pytest.raises(SystemExit) as removed:
        main(["--directory", str(branch), "check-compatibility", "--base", str(base)])
    assert removed.value.code == 1
    assert "REMOVED: cases.note.added.v2" in capsys.readouterr().out

    (branch / "diagnostics.ping.requested.v1.json").write_text('{"title": "BREAKING"}')
    with pytest.raises(SystemExit) as breaking:
        main(["--directory", str(branch), "check-compatibility"])
    assert breaking.value.code == 1
    assert "INCOMPATIBLE: diagnostics.ping.requested.v1" in capsys.readouterr().out


def test_an_unreachable_registry_is_reported_without_a_traceback(
    tmp_path: Path, fresh_settings: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Nothing listens on port 1, so connecting fails at once.
    fresh_settings.setenv("FINGUARD_SCHEMA_REGISTRY__URL", "http://127.0.0.1:1/apis/ccompat/v7")
    write(tmp_path, [PING_REQUESTED_CONTRACT])

    with pytest.raises(SystemExit) as failure:
        main(["--directory", str(tmp_path), "register"])

    assert failure.value.code == 2
    error = capsys.readouterr().err
    assert error.startswith("error: PUT /config/diagnostics.ping.requested.v1:")
    assert "http://127.0.0.1:1/apis/ccompat/v7 is unreachable" in error


def test_a_missing_contracts_directory_is_an_error_not_created(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as failure:
        main(["--directory", str(tmp_path / "contracts"), "generate"])

    assert failure.value.code == 2
    assert not (tmp_path / "contracts").exists()
    assert "not a directory" in capsys.readouterr().err

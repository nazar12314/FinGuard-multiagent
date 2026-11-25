import json
from pathlib import Path

import pytest
from pydantic import BaseModel, ConfigDict

from finguard.core.contracts import CONTRACTS, read, render, stale, write
from finguard.core.diagnostics import PING_REQUESTED_CONTRACT
from finguard.entrypoints.contracts import main
from finguard.platform.contracts import MessageContract, message_schema

REPOSITORY_CONTRACTS = Path(__file__).parents[3] / "contracts"


class Note(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str


class OpenNote(BaseModel):
    text: str


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


def test_a_data_model_that_accepts_unknown_fields_is_refused() -> None:
    open_contract = MessageContract(message_type="cases.note.added", version=3, data=OpenNote)

    with pytest.raises(ValueError, match="extra='forbid'"):
        message_schema(open_contract)


def test_the_committed_contracts_match_the_message_models() -> None:
    assert stale(REPOSITORY_CONTRACTS, CONTRACTS) == []


def test_generating_writes_missing_files_and_removes_retired_ones(tmp_path: Path) -> None:
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


def test_missing_outdated_and_retired_files_are_reported(tmp_path: Path) -> None:
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

import csv
from pathlib import Path

import pytest

from finguard.modules.screening.adapters.opensanctions import read_opensanctions
from finguard.platform.errors import ValidationFailed

HEADER = [
    "id", "schema", "name", "aliases", "birth_date", "countries", "addresses", "identifiers",
    "sanctions", "phones", "emails", "program_ids", "dataset", "first_seen", "last_seen",
    "last_change",
]  # fmt: skip


def write_csv(path: Path, rows: list[dict[str, str]], header: list[str] = HEADER) -> Path:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=header, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in header})
    return path


def target(entity_id: str, name: str, **fields: str) -> dict[str, str]:
    return {"id": entity_id, "schema": "Person", "name": name, **fields}


def test_reads_both_collections_and_flags_where_each_entity_is_listed(tmp_path: Path) -> None:
    sanctions = write_csv(
        tmp_path / "sanctions.csv",
        [
            target(
                "NK-1",
                "Petr Ivanov",
                aliases="Ivanov Petr; Pyotr Ivanov",
                birth_date="1965-03-02;1965",
                countries="ru",
                program_ids="EU-RUS",
                dataset="EU Sanctions",
            ),
            target("NK-2", "Both Lists", dataset="EU Sanctions"),
        ],
    )
    peps = write_csv(
        tmp_path / "peps.csv",
        [
            target("Q-1", "Anna Kowalska", countries="pl", dataset="Wikidata PEPs"),
            target("NK-2", "Both Lists", aliases="B. Lists", dataset="Wikidata PEPs"),
        ],
    )

    entities = {e.entity_id: e for e in read_opensanctions(sanctions, peps)}

    assert entities["NK-1"].aliases == ("Ivanov Petr", "Pyotr Ivanov")
    assert entities["NK-1"].birth_dates == ("1965-03-02", "1965")
    assert (entities["NK-1"].sanctioned, entities["NK-1"].pep) == (True, False)
    assert (entities["Q-1"].sanctioned, entities["Q-1"].pep) == (False, True)
    both = entities["NK-2"]
    assert (both.sanctioned, both.pep) == (True, True)
    assert both.aliases == ("B. Lists",)
    assert both.datasets == ("EU Sanctions", "Wikidata PEPs")


def test_keeps_only_people_and_organisations_with_a_name(tmp_path: Path) -> None:
    sanctions = write_csv(
        tmp_path / "sanctions.csv",
        [
            target("V-1", "Abrau", schema="Vessel"),
            target("C-1", "Acme Trading LLC", schema="Company"),
            target("P-0", ""),
        ],
    )
    peps = write_csv(tmp_path / "peps.csv", [])

    assert [e.entity_id for e in read_opensanctions(sanctions, peps)] == ["C-1"]


def test_rejects_a_file_without_the_expected_columns(tmp_path: Path) -> None:
    sanctions = write_csv(tmp_path / "sanctions.csv", [], header=["id", "name"])
    peps = write_csv(tmp_path / "peps.csv", [])

    with pytest.raises(ValidationFailed, match="lacks the columns"):
        list(read_opensanctions(sanctions, peps))

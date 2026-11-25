from pathlib import Path

import polars as pl
import pytest
from tests.unit.datagen.amlworld_files import (
    OUR_BANK,
    attempt,
    csv_row,
    write_patterns_file,
    write_transactions_csv,
)

from datagen.amlworld import AmlworldFormatError, read_bank_rows
from datagen.ground_truth import GroundTruth, build_ground_truth, read_attempts

FAN_OUT = [
    csv_row(OUR_BANK, "M1", "002", "X1", laundering=True),
    csv_row(OUR_BANK, "M1", "003", "X2", laundering=True),
    csv_row("004", "Z1", "005", "Z2", laundering=True),
]
ELSEWHERE = [csv_row("006", "Y1", "007", "Y2", laundering=True)]
LAUNDERING_OUTSIDE_ATTEMPTS = csv_row(OUR_BANK, "L1", "002", "X1", laundering=True)
NORMAL = csv_row(OUR_BANK, "C1", OUR_BANK, "C1")


def ground_truth(tmp_path: Path, csv_rows: list[str], attempts: list[list[str]]) -> GroundTruth:
    csv = write_transactions_csv(tmp_path, csv_rows)
    patterns = write_patterns_file(tmp_path, attempts)
    return build_ground_truth(read_bank_rows(csv, OUR_BANK), read_attempts(patterns), OUR_BANK)


@pytest.fixture
def truth(tmp_path: Path) -> GroundTruth:
    return ground_truth(
        tmp_path,
        [*FAN_OUT, *ELSEWHERE, LAUNDERING_OUTSIDE_ATTEMPTS, NORMAL],
        [
            attempt("FAN-OUT", FAN_OUT, "Max 3-degree Fan-Out"),
            attempt("BIPARTITE", ELSEWHERE),
        ],
    )


def test_reads_attempts_in_file_order(tmp_path: Path) -> None:
    patterns = write_patterns_file(
        tmp_path,
        [attempt("FAN-OUT", FAN_OUT, "Max 3-degree Fan-Out"), attempt("BIPARTITE", ELSEWHERE)],
    )

    attempt_rows = read_attempts(patterns)

    assert attempt_rows.select("attempt_id", "pattern_type", "description").unique(
        maintain_order=True
    ).rows() == [
        ("attempt-00001", "fan_out", "Max 3-degree Fan-Out"),
        ("attempt-00002", "bipartite", None),
    ]
    assert attempt_rows.height == 4


@pytest.mark.parametrize(
    ("lines", "message"),
    [
        (attempt("ZIGZAG", FAN_OUT), "unknown pattern ZIGZAG"),
        (["BEGIN LAUNDERING ATTEMPT - STACK", *FAN_OUT], "never ends"),
        (FAN_OUT, "expected a transaction inside an attempt"),
        (["BEGIN LAUNDERING ATTEMPT - STACK", "END LAUNDERING ATTEMPT - CYCLE"], "doesn't close"),
        (["BEGIN LAUNDERING ATTEMPT - STACK", "2022/09/01 10:00,012,M1"], "expected a transaction"),
    ],
)
def test_rejects_a_malformed_pattern_file(tmp_path: Path, lines: list[str], message: str) -> None:
    patterns = write_patterns_file(tmp_path, [lines])

    with pytest.raises(AmlworldFormatError, match=message):
        read_attempts(patterns)


def test_labels_name_the_attempt_a_transaction_belongs_to(truth: GroundTruth) -> None:
    assert truth.transaction_labels.rows() == [
        ("tx-000000000", True, "attempt-00001"),
        ("tx-000000001", True, "attempt-00001"),
        ("tx-000000004", True, None),
        ("tx-000000005", False, None),
    ]


def test_keeps_attempts_that_reach_our_bank_with_the_part_we_see(truth: GroundTruth) -> None:
    assert truth.laundering_attempts.rows(named=True) == [
        {
            "attempt_id": "attempt-00001",
            "pattern_type": "fan_out",
            "description": "Max 3-degree Fan-Out",
            "transaction_count": 3,
            "visible_transaction_count": 2,
        }
    ]


def test_labels_every_account_with_its_laundering_and_attempts(truth: GroundTruth) -> None:
    labels = {row[0]: row[1:] for row in truth.account_labels.rows()}

    assert labels["12:M1"] == (2, ["attempt-00001"])
    assert labels["2:X1"] == (2, ["attempt-00001"])
    assert labels["12:L1"] == (1, [])
    assert labels["12:C1"] == (0, [])


def test_rejects_an_attempt_transaction_missing_from_the_csv(tmp_path: Path) -> None:
    with pytest.raises(AmlworldFormatError, match="1 pattern-file transactions of bank 12"):
        ground_truth(tmp_path, [NORMAL], [attempt("FAN-OUT", FAN_OUT[:1])])


def test_rejects_a_transaction_in_two_attempts(tmp_path: Path) -> None:
    with pytest.raises(AmlworldFormatError, match="more than one laundering attempt"):
        ground_truth(
            tmp_path,
            FAN_OUT,
            [attempt("FAN-OUT", FAN_OUT[:1]), attempt("STACK", FAN_OUT[:1])],
        )


def test_an_empty_pattern_file_leaves_every_transaction_outside_attempts(tmp_path: Path) -> None:
    truth = ground_truth(tmp_path, [LAUNDERING_OUTSIDE_ATTEMPTS], [])

    assert truth.laundering_attempts.is_empty()
    assert truth.transaction_labels.get_column("attempt_id").to_list() == [None]
    assert truth.transaction_labels.schema["attempt_id"] == pl.String

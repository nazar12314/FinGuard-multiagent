from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from tests.unit.datagen.amlworld_files import OUR_BANK, csv_row, write_transactions_csv

from datagen.amlworld import AmlworldFormatError, load_bank_slice


def test_keeps_only_transactions_our_bank_is_party_to(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path,
        [
            csv_row("001", "A1", "002", "B1"),
            csv_row(OUR_BANK, "C1", "002", "B1"),
            csv_row("001", "A1", OUR_BANK, "C2"),
            csv_row(OUR_BANK, "C1", OUR_BANK, "C2"),
        ],
    )

    transactions = load_bank_slice(csv, OUR_BANK).transactions

    assert transactions.select("from_account_id", "to_account_id").rows() == [
        ("12:C1", "2:B1"),
        ("1:A1", "12:C2"),
        ("12:C1", "12:C2"),
    ]


def test_transaction_ids_are_positions_in_the_full_file(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path,
        [csv_row("001", "A1", "002", "B1"), csv_row(OUR_BANK, "C1", "002", "B1")],
    )

    transactions = load_bank_slice(csv, OUR_BANK).transactions

    assert transactions.get_column("transaction_id").to_list() == ["tx-000000001"]


def test_an_account_is_identified_by_bank_and_number(tmp_path: Path) -> None:
    csv = write_transactions_csv(tmp_path, [csv_row(OUR_BANK, "SAME", "002", "SAME")])

    accounts = load_bank_slice(csv, OUR_BANK).accounts

    assert accounts.rows() == [
        ("12:SAME", "12", "SAME", True),
        ("2:SAME", "2", "SAME", False),
    ]


def test_zero_padded_spellings_of_a_bank_id_are_one_bank(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path, [csv_row("012", "C1", "002", "B1"), csv_row("0012", "D1", "0002", "B2")]
    )

    accounts = load_bank_slice(csv, "12").accounts

    assert accounts.filter("held_at_our_bank").get_column("account_id").to_list() == [
        "12:C1",
        "12:D1",
    ]
    assert set(accounts.get_column("bank_id")) == {"12", "2"}


def test_rejects_a_bank_id_that_isnt_a_number(tmp_path: Path) -> None:
    csv = write_transactions_csv(tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1")])

    with pytest.raises(AmlworldFormatError, match="bank ids are numbers"):
        load_bank_slice(csv, "twelve")


@pytest.mark.parametrize(
    ("amount", "currency", "code", "eur"),
    [
        ("100.00", "Euro", "EUR", "100.00"),
        ("100.00", "Yen", "JPY", "0.70"),
        ("0.000001", "Bitcoin", "BTC", "0.02"),
        ("2.50", "UK Pound", "GBP", "2.88"),
    ],
)
def test_converts_amounts_to_eur_and_keeps_the_originals(
    tmp_path: Path, amount: str, currency: str, code: str, eur: str
) -> None:
    csv = write_transactions_csv(
        tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1", amount=amount, currency=currency)]
    )

    [transaction] = load_bank_slice(csv, OUR_BANK).transactions.rows(named=True)

    assert transaction["amount_paid"] == Decimal(amount)
    assert transaction["payment_currency"] == code
    assert transaction["amount_paid_eur"] == Decimal(eur)


def test_converts_each_side_in_its_own_currency(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path,
        [csv_row(OUR_BANK, "C1", "002", "B1", currency="US Dollar", received_currency="Yen")],
    )

    [transaction] = load_bank_slice(csv, OUR_BANK).transactions.rows(named=True)

    assert transaction["receiving_currency"] == "JPY"
    assert transaction["amount_received_eur"] == Decimal("0.70")
    assert transaction["amount_paid_eur"] == Decimal("100.00")


def test_parses_timestamps_as_utc_and_orders_by_time(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path,
        [
            csv_row(OUR_BANK, "C1", "002", "B1", timestamp="2022/09/02 08:30"),
            csv_row(OUR_BANK, "C1", "002", "B1", timestamp="2022/09/01 23:59"),
            csv_row(OUR_BANK, "C2", "002", "B1", timestamp="2022/09/01 23:59"),
        ],
    )

    transactions = load_bank_slice(csv, OUR_BANK).transactions

    assert transactions.select("transaction_id", "booked_at").rows() == [
        ("tx-000000001", datetime(2022, 9, 1, 23, 59, tzinfo=UTC)),
        ("tx-000000002", datetime(2022, 9, 1, 23, 59, tzinfo=UTC)),
        ("tx-000000000", datetime(2022, 9, 2, 8, 30, tzinfo=UTC)),
    ]


def test_names_payment_formats_in_snake_case(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1", payment_format="Credit Card")]
    )

    transactions = load_bank_slice(csv, OUR_BANK).transactions

    assert transactions.get_column("payment_format").to_list() == ["credit_card"]


def test_labels_are_kept_apart_from_the_transactions(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path,
        [
            csv_row(OUR_BANK, "C1", "002", "B1", laundering=True),
            csv_row(OUR_BANK, "C1", "002", "B2"),
        ],
    )

    bank_slice = load_bank_slice(csv, OUR_BANK)

    assert "is_laundering" not in bank_slice.transactions.columns
    assert bank_slice.labels.rows() == [("tx-000000000", True), ("tx-000000001", False)]


def test_rejects_an_unknown_currency(tmp_path: Path) -> None:
    csv = write_transactions_csv(
        tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1", currency="Doubloon")]
    )

    with pytest.raises(AmlworldFormatError, match="Doubloon"):
        load_bank_slice(csv, OUR_BANK)


def test_rejects_a_file_with_another_header(tmp_path: Path) -> None:
    csv = tmp_path / "HI-Small_Trans.csv"
    csv.write_text("Timestamp,Amount\n2022/09/01 10:00,1.00\n")

    with pytest.raises(AmlworldFormatError, match="unexpected header"):
        load_bank_slice(csv, OUR_BANK)


def test_rejects_a_bank_without_transactions(tmp_path: Path) -> None:
    csv = write_transactions_csv(tmp_path, [csv_row("001", "A1", "002", "B1")])

    with pytest.raises(AmlworldFormatError, match="bank 12 has no transactions"):
        load_bank_slice(csv, OUR_BANK)


def test_reports_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Kaggle"):
        load_bank_slice(tmp_path / "HI-Small_Trans.csv", OUR_BANK)

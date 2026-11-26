import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from finguard.modules.ingestion.messages import AccountReference, Money

SCHEMA = json.loads(Path("contracts/transaction.booked.v1.json").read_text())
AMOUNT_PATTERN = SCHEMA["$defs"]["Money"]["properties"]["amount"]["pattern"]


@pytest.mark.parametrize("amount", ["0.0000001", "1E+3", "16.080000", "250"])
def test_an_amount_serialises_in_the_form_the_published_schema_allows(amount: str) -> None:
    serialised = json.loads(Money(amount=Decimal(amount), currency="EUR").model_dump_json())

    assert re.match(AMOUNT_PATTERN, serialised["amount"])
    assert Decimal(serialised["amount"]) == Decimal(amount)


def test_an_account_is_identified_together_with_its_bank() -> None:
    assert AccountReference(bank_id="12", account_number="8001167D0").account_id == "12:8001167D0"

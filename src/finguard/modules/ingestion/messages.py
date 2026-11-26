from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from finguard.platform.contracts import MessageContract

TRANSACTION_BOOKED = "transaction.booked"

PaymentFormat = Literal["ach", "bitcoin", "cash", "cheque", "credit_card", "reinvestment", "wire"]


class AccountReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bank_id: str = Field(min_length=1)
    account_number: str = Field(min_length=1)

    @property
    def account_id(self) -> str:
        # Account numbers repeat across banks, so an account is only unique with its bank.
        return f"{self.bank_id}:{self.account_number}"


class Money(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The pattern carries the positive-decimal rule into the published JSON Schema, where the
    # amount is a string.
    amount: Decimal = Field(gt=0, json_schema_extra={"pattern": r"^(?!0+(\.0+)?$)\d+(\.\d+)?$"})
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class TransactionBooked(BaseModel):
    """A transaction our bank is party to, as the core banking system booked it.

    Amounts are in their original currencies; ingestion converts them to EUR. The core system
    re-sends a booking with the same transaction_id, so that id, not the envelope's, is what
    makes a booking unique.
    """

    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1)
    booked_at: AwareDatetime
    from_account: AccountReference
    to_account: AccountReference
    amount_paid: Money
    amount_received: Money
    payment_format: PaymentFormat


TRANSACTION_BOOKED_CONTRACT = MessageContract(
    message_type=TRANSACTION_BOOKED, version=1, data=TransactionBooked
)

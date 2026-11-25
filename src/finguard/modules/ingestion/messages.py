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


class Money(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class TransactionBooked(BaseModel):
    """A transaction our bank is party to, as the core banking system booked it.

    Amounts are in their original currencies; ingestion converts them to EUR.
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

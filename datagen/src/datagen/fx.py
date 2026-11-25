from decimal import Decimal

import polars as pl

# Fixed synthetic rates, roughly those of September 2022 when the AMLworld transactions are dated.
# They never change, so a world converts the same way every time it's built.
EUR_PER_UNIT: dict[str, Decimal] = {
    "AUD": Decimal("0.68"),
    "BRL": Decimal("0.19"),
    "BTC": Decimal("19500"),
    "CAD": Decimal("0.76"),
    "CHF": Decimal("1.04"),
    "CNY": Decimal("0.145"),
    "EUR": Decimal("1"),
    "GBP": Decimal("1.15"),
    "ILS": Decimal("0.29"),
    "INR": Decimal("0.0125"),
    "JPY": Decimal("0.007"),
    "MXN": Decimal("0.05"),
    "RUB": Decimal("0.0167"),
    "SAR": Decimal("0.266"),
    "USD": Decimal("1.00"),
}

RATE = pl.Decimal(18, 8)
EUR_AMOUNT = pl.Decimal(24, 2)


def to_eur(amount: pl.Expr, currency: pl.Expr) -> pl.Expr:
    rate = currency.replace_strict(EUR_PER_UNIT, return_dtype=RATE)
    return (amount * rate).round(2).cast(EUR_AMOUNT)

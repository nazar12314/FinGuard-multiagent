import re
import unicodedata
from dataclasses import dataclass

import jellyfish

# OpenSanctions schemas we screen customers and counterparties against; vessels, aircraft,
# wallets and securities can't hold an account.
SCREENED_SCHEMAS = frozenset({"Person", "Organization", "Company", "LegalEntity"})

_NOT_ALPHANUMERIC = re.compile(r"[^0-9a-z]+")


@dataclass(frozen=True, slots=True)
class WatchlistEntity:
    entity_id: str
    schema: str
    name: str
    aliases: tuple[str, ...]
    birth_dates: tuple[str, ...]
    countries: tuple[str, ...]
    program_ids: tuple[str, ...]
    datasets: tuple[str, ...]
    sanctioned: bool
    pep: bool

    def names(self) -> tuple[str, ...]:
        return (self.name, *(alias for alias in self.aliases if alias != self.name))


@dataclass(frozen=True, slots=True)
class NameForms:
    """A name in the forms the candidate search compares."""

    normalised: str
    token_sorted: str
    phonetic: str


@dataclass(frozen=True, slots=True)
class WatchlistName:
    entity_id: str
    name: str
    latin: str
    forms: NameForms
    is_alias: bool


def name_forms(latin: str) -> NameForms:
    """Forms of a name already written in Latin letters: casefolded, accents and punctuation
    removed; its tokens sorted, so "Ivanov Petr" meets "Petr Ivanov"; and a phonetic key per
    token, so "Mohammed" meets "Muhamad"."""
    decomposed = unicodedata.normalize("NFKD", latin)
    unaccented = "".join(char for char in decomposed if not unicodedata.combining(char))
    normalised = _NOT_ALPHANUMERIC.sub(" ", unaccented.casefold()).strip()
    tokens = normalised.split()
    return NameForms(
        normalised=normalised,
        token_sorted=" ".join(sorted(tokens)),
        phonetic=" ".join(jellyfish.metaphone(token) for token in tokens),
    )

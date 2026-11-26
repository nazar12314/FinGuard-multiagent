import re
import unicodedata
from dataclasses import dataclass

import jellyfish

# OpenSanctions schemas we screen customers and counterparties against; vessels, aircraft,
# wallets and securities can't hold an account.
SCREENED_SCHEMAS = frozenset({"Person", "Organization", "Company", "LegalEntity"})

_NOT_ALPHANUMERIC = re.compile(r"[^0-9a-z]+")
# Marks that sit inside a word: apostrophes (O'Brien), the soft and hard signs of Cyrillic
# romanisations (Ignat'yev, Ignat"ev) and BGN's separating dot (Mostsov written Mosts·ov).
_INSIDE_A_WORD = re.compile("['\"`\u00b4\u00b7\u02b9\u02ba\u02bb\u02bc\u02be\u02bf\u2018\u2019]")


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
    decomposed = unicodedata.normalize("NFKD", _INSIDE_A_WORD.sub("", latin))
    unaccented = "".join(char for char in decomposed if not unicodedata.combining(char))
    normalised = _NOT_ALPHANUMERIC.sub(" ", unaccented.casefold()).strip()
    tokens = normalised.split()
    return NameForms(
        normalised=normalised,
        token_sorted=" ".join(sorted(tokens)),
        # Metaphone gives nothing for some tokens (digits, "II"); they'd leave empty slots.
        phonetic=" ".join(key for token in tokens if (key := jellyfish.metaphone(token))),
    )

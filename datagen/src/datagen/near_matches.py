import random
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from finguard.modules.screening.adapters.icu_transliterator import IcuTransliterator
from finguard.modules.screening.domain.watchlist import name_forms

# A screening hit is a true match when our person is the listed one (name variant, same birth
# date and nationality) and a false positive when only the name is close. True matches are
# far likelier around laundering, namesakes fall on anyone.
TRUE_MATCH_WHEN_LAUNDERING = 0.1
TRUE_MATCH_WHEN_BENIGN = 0.002
NAMESAKE = 0.02
# Companies only get namesakes: an EU-registered SME is rarely the listed organisation itself.
COMPANY_NAMESAKE = 0.01
# Most true matches are sanctioned; politically exposed people run businesses too.
TRUE_MATCH_FROM_SANCTIONS = 0.7

SPELLING_SWAPS = [("kh", "h"), ("y", "i"), ("ou", "u"), ("sch", "sh"), ("ph", "f"), ("ei", "ey")]
_FULL_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_YEAR = re.compile(r"^(\d{4})")
_PUNCTUATION = re.compile(r"[^\w\s-]")
# Legal-form words of listed organisations, dropped so a namesake reads like an ordinary SME.
LEGAL_WORDS = frozenset(
    [
        "llc",
        "ltd",
        "limited",
        "liability",
        "company",
        "joint",
        "stock",
        "jsc",
        "pjsc",
        "ojsc",
        "cjsc",
        "zao",
        "ooo",
        "oao",
        "tov",
        "inc",
        "co",
        "corp",
        "corporation",
        "group",
        "holding",
        "sa",
        "ag",
        "gmbh",
    ]
)


@dataclass(frozen=True)
class NearMatches:
    person_changes: pl.DataFrame
    company_changes: pl.DataFrame
    labels: pl.DataFrame


def opensanctions_files(directory: Path) -> dict[str, Path]:
    return {
        "sanctions": directory / "sanctions.targets.simple.csv",
        "peps": directory / "peps.targets.simple.csv",
    }


def read_listed(directory: Path) -> pl.DataFrame:
    """Listed people and organisations with a usable Latin name, from both collections."""
    frames = []
    for collection, path in opensanctions_files(directory).items():
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} not found; download targets.simple.csv from "
                f"https://data.opensanctions.org/datasets/latest/{collection}/"
            )
        frames.append(
            pl.scan_csv(path, infer_schema=False)
            .select("id", "schema", "name", "birth_date", "countries")
            .filter(pl.col("schema").is_in(["Person", "Organization", "Company"]))
            .with_columns(collection=pl.lit(collection))
            .collect()
        )
    # An entity on both lists counts as sanctioned.
    return (
        pl.concat(frames)
        .sort("id", "collection", descending=[False, True])
        .unique("id", keep="first", maintain_order=True)
    )


def plant_near_matches(
    persons: pl.DataFrame,
    company_customers: pl.DataFrame,
    laundering_people: set[str],
    listed: pl.DataFrame,
    seed: int,
) -> NearMatches:
    rng = random.Random(f"{seed}:near-matches")
    transliterator = IcuTransliterator()
    people = listed.filter(pl.col("schema") == "Person")
    sanctioned_people = _Pool(people.filter(pl.col("collection") == "sanctions"), transliterator)
    peps = _Pool(people.filter(pl.col("collection") == "peps"), transliterator)
    everyone = _Pool(people, transliterator)
    organisations = _Pool(
        listed.filter(pl.col("schema") != "Person"), transliterator, organisations=True
    )

    person_changes: list[dict[str, object]] = []
    company_changes: list[dict[str, object]] = []
    labels: list[dict[str, object]] = []
    for person in persons.sort("person_id").rows(named=True):
        true_rate = (
            TRUE_MATCH_WHEN_LAUNDERING
            if person["person_id"] in laundering_people
            else TRUE_MATCH_WHEN_BENIGN
        )
        draw = rng.random()
        if draw < true_rate:
            pool = sanctioned_people if rng.random() < TRUE_MATCH_FROM_SANCTIONS else peps
            listed_person = pool.pick(rng, with_birth_date=True)
            label = "true_match"
        elif draw < true_rate + NAMESAKE:
            listed_person = everyone.pick(rng)
            label = "false_positive"
        else:
            continue
        name, variants = _variant(rng, listed_person, transliterator)
        birth_date, nationality = _identity(rng, listed_person, person, label)
        person_changes.append(
            {
                "person_id": person["person_id"],
                "full_name": name,
                "full_name_latin": name,
                "birth_date": birth_date,
                "nationality": nationality,
            }
        )
        labels.append(_label("person", person["person_id"], listed_person, label, name, variants))

    for company in company_customers.sort("customer_id").rows(named=True):
        if rng.random() >= COMPANY_NAMESAKE:
            continue
        label = "false_positive"
        listed_company = organisations.pick(rng)
        name, variants = _variant(rng, listed_company, transliterator)
        name = f"{name} {company['legal_form']}"
        company_changes.append({"customer_id": company["customer_id"], "name": name})
        labels.append(
            _label("company", company["customer_id"], listed_company, label, name, variants)
        )

    return NearMatches(
        person_changes=pl.DataFrame(person_changes, schema=PERSON_CHANGE_SCHEMA),
        company_changes=pl.DataFrame(company_changes, schema=COMPANY_CHANGE_SCHEMA),
        labels=pl.DataFrame(labels, schema=SCREENING_LABEL_SCHEMA).sort("subject_id"),
    )


@dataclass(frozen=True)
class _Listed:
    entity_id: str
    collection: str
    name: str
    latin_spellings: list[str]
    birth_date: str | None
    country: str | None


class _Pool:
    """Listed entities to draw from, transliterated only when drawn."""

    def __init__(
        self, rows: pl.DataFrame, transliterator: IcuTransliterator, *, organisations: bool = False
    ) -> None:
        self._rows = rows.sort("id")
        self._transliterator = transliterator
        self._organisations = organisations

    def pick(self, rng: random.Random, *, with_birth_date: bool = False) -> _Listed:
        for _ in range(10_000):
            row = self._rows.row(rng.randrange(self._rows.height), named=True)
            listed = self._usable(row)
            if listed and (listed.birth_date or not with_birth_date):
                return listed
        raise ValueError("no usable listed entity found")

    def _usable(self, row: dict[str, str | None]) -> _Listed | None:
        spellings = [
            " ".join(self._meaningful(_PUNCTUATION.sub("", spelling).split()))
            for spelling in self._transliterator.latin_spellings(row["name"] or "")
        ]
        tokens = spellings[0].split()
        fewest = 1 if self._organisations else 2
        if not (fewest <= len(tokens) <= 4) or not spellings[0].isascii():
            return None
        if min(map(len, tokens)) < 2:
            return None
        birth_dates = [d for d in (row["birth_date"] or "").split(";") if d]
        countries = [c.upper() for c in (row["countries"] or "").split(";") if c]
        return _Listed(
            entity_id=str(row["id"]),
            collection=str(row["collection"]),
            name=str(row["name"]),
            latin_spellings=list(dict.fromkeys(spellings)),
            birth_date=birth_dates[0] if birth_dates else None,
            country=countries[0] if countries else None,
        )

    def _meaningful(self, tokens: list[str]) -> list[str]:
        if not self._organisations:
            return tokens
        return [token for token in tokens if token.casefold() not in LEGAL_WORDS]


def _variant(
    rng: random.Random, listed: _Listed, transliterator: IcuTransliterator
) -> tuple[str, list[str]]:
    """A name close to the listed one, the way it turns up on onboarding forms."""
    tokens = [token.title() for token in listed.latin_spellings[0].split()]
    variations: dict[str, Callable[[list[str]], list[str] | None]] = {
        "other_spelling": lambda t: _other_spelling(rng, listed, t),
        "swapped_order": lambda t: t[::-1],
        "missing_middle_name": lambda t: [t[0], t[-1]] if len(t) >= 3 else None,
        "typo": lambda t: _typo(rng, t),
    }
    chosen = rng.sample(list(variations), rng.choice([1, 1, 2]))
    applied: list[str] = []
    for variation in chosen:
        changed = variations[variation](tokens)
        if changed and changed != tokens:
            tokens, applied = changed, [*applied, variation]
    if not applied:
        tokens, applied = _typo(rng, tokens) or tokens, ["typo"]
    name = " ".join(tokens)
    assert name_forms(name).normalised, listed.name
    return name, applied


def _other_spelling(rng: random.Random, listed: _Listed, tokens: list[str]) -> list[str] | None:
    if len(listed.latin_spellings) > 1:
        return [token.title() for token in rng.choice(listed.latin_spellings[1:]).split()]
    swaps = [(a, b) for a, b in SPELLING_SWAPS if a in " ".join(tokens).lower()]
    if not swaps:
        return None
    old, new = rng.choice(swaps)
    return [re.sub(old, new, token, flags=re.IGNORECASE).title() for token in tokens]


def _typo(rng: random.Random, tokens: list[str]) -> list[str] | None:
    long_tokens = [index for index, token in enumerate(tokens) if len(token) >= 4]
    if not long_tokens:
        return None
    index = rng.choice(long_tokens)
    token = tokens[index]
    position = rng.randrange(1, len(token) - 1)
    if rng.random() < 0.5:
        typo = token[:position] + token[position + 1 :]
    else:
        typo = token[:position] + token[position + 1] + token[position] + token[position + 2 :]
    return [*tokens[:index], typo, *tokens[index + 1 :]]


def _identity(
    rng: random.Random, listed: _Listed, person: dict[str, object], label: str
) -> tuple[date, str]:
    generated_birth = person["birth_date"]
    assert isinstance(generated_birth, date)
    listed_birth = _birth_date(rng, listed.birth_date)
    if label == "true_match":
        return listed_birth or generated_birth, listed.country or str(person["nationality"])
    # A namesake is someone else: born years apart from the listed person.
    if listed_birth and abs((listed_birth - generated_birth).days) < 3 * 365:
        generated_birth = listed_birth - timedelta(
            days=rng.choice([-1, 1]) * rng.randint(4, 15) * 365
        )
    return generated_birth, str(person["nationality"])


def _birth_date(rng: random.Random, listed: str | None) -> date | None:
    if not listed:
        return None
    if _FULL_DATE.match(listed):
        return date.fromisoformat(listed)
    if year := _YEAR.match(listed):
        return date(int(year.group(1)), 1, 1) + timedelta(days=rng.randint(0, 364))
    return None


def _label(
    subject_type: str, subject_id: str, listed: _Listed, label: str, name: str, variants: list[str]
) -> dict[str, object]:
    return {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "watchlist_entity_id": listed.entity_id,
        "collection": listed.collection,
        "label": label,
        "listed_name": listed.name,
        "planted_name": name,
        "variants": variants,
    }


PERSON_CHANGE_SCHEMA = pl.Schema(
    {
        "person_id": pl.String(),
        "full_name": pl.String(),
        "full_name_latin": pl.String(),
        "birth_date": pl.Date(),
        "nationality": pl.String(),
    }
)
COMPANY_CHANGE_SCHEMA = pl.Schema({"customer_id": pl.String(), "name": pl.String()})
SCREENING_LABEL_SCHEMA = pl.Schema(
    {
        "subject_type": pl.String(),
        "subject_id": pl.String(),
        "watchlist_entity_id": pl.String(),
        "collection": pl.String(),
        "label": pl.String(),
        "listed_name": pl.String(),
        "planted_name": pl.String(),
        "variants": pl.List(pl.String),
    }
)

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from itertools import pairwise

import polars as pl
from faker import Faker

from datagen.customers import (
    COUNTRIES,
    PERSON_SCHEMA,
    Country,
    generate_person,
    locale_fakers,
    weighted_choice,
)

# Share of laundering-linked companies with a shell-like structure, and of benign companies
# with a look-alike one for an innocent reason, drawn from the same mix of flags.
SHELL_LIKE_WHEN_LAUNDERING = 0.5
HARD_NEGATIVE_WHEN_BENIGN = 0.08
# Benign groups often own their trading company through a domestic holding company.
BENIGN_HOLDING_COMPANY = 0.15

OFFSHORE_HOLDING = "offshore_holding"
NOMINEE_DIRECTOR = "nominee_director"
CIRCULAR_OWNERSHIP = "circular_ownership"
STRUCTURE_MIXES: list[tuple[tuple[str, ...], float]] = [
    ((OFFSHORE_HOLDING,), 0.3),
    ((NOMINEE_DIRECTOR,), 0.25),
    ((CIRCULAR_OWNERSHIP,), 0.1),
    ((OFFSHORE_HOLDING, NOMINEE_DIRECTOR), 0.2),
    ((OFFSHORE_HOLDING, CIRCULAR_OWNERSHIP), 0.1),
    ((OFFSHORE_HOLDING, NOMINEE_DIRECTOR, CIRCULAR_OWNERSHIP), 0.05),
]
HARD_NEGATIVE_STORIES = {
    OFFSHORE_HOLDING: "international group with a holding company abroad",
    NOMINEE_DIRECTOR: "directors provided by a corporate services firm",
    CIRCULAR_OWNERSHIP: "cross-holdings inside a family group",
}

# Nominee directors sit on many boards. Corporate services firms appoint the same people to
# legitimate companies too, so the pool is shared and a director's name doesn't give the label.
NOMINEE_POOL_SIZE = 12


@dataclass(frozen=True)
class Jurisdiction:
    code: str
    company_form: str
    registration_format: str


OFFSHORE = [
    Jurisdiction("CY", "Ltd", "HE{:06d}"),
    Jurisdiction("MT", "Ltd", "C{:05d}"),
    Jurisdiction("AE", "FZE", "{:07d}"),
    Jurisdiction("VG", "Ltd", "{:07d}"),
    Jurisdiction("SC", "Ltd", "{:06d}"),
]
REGISTRATION_FORMATS = {
    "DE": "HRB {:06d}",
    "PL": "KRS 0000{:06d}",
    "NL": "KVK {:08d}",
    "RO": "J40/{:05d}/2015",
    "IE": "CRO {:06d}",
    "FR": "SIREN {:09d}",
    "ES": "CIF B{:08d}",
}


@dataclass(frozen=True)
class Companies:
    companies: pl.DataFrame
    officers: pl.DataFrame
    shareholdings: pl.DataFrame
    persons: pl.DataFrame
    labels: pl.DataFrame


def generate_companies(
    customers: pl.DataFrame, customer_labels: pl.DataFrame, activity_starts: date, seed: int
) -> Companies:
    builder = _Builder(
        rng=random.Random(f"{seed}:companies"),
        fakers=locale_fakers(seed, "companies"),
        activity_starts=activity_starts,
    )
    ireland = next(country for country in COUNTRIES if country.code == "IE")
    builder.nominees = [builder.new_person(ireland) for _ in range(NOMINEE_POOL_SIZE)]
    laundering = dict(customer_labels.select("customer_id", "laundering_linked").iter_rows())
    for customer in (
        customers.filter(pl.col("customer_type") == "company").sort("customer_id").rows(named=True)
    ):
        builder.add_customer_company(customer, laundering[customer["customer_id"]])
    return builder.companies_frames()


@dataclass
class _Builder:
    rng: random.Random
    fakers: dict[str, Faker]
    activity_starts: date
    nominees: list[str] = field(default_factory=list)
    companies: list[dict[str, object]] = field(default_factory=list)
    officers: list[dict[str, object]] = field(default_factory=list)
    shareholdings: list[dict[str, object]] = field(default_factory=list)
    persons: list[dict[str, object]] = field(default_factory=list)
    labels: list[dict[str, object]] = field(default_factory=list)
    holding_companies: int = 0

    def add_customer_company(self, customer: dict[str, object], laundering_linked: bool) -> None:
        flags: tuple[str, ...] = ()
        structure = "plain"
        if self.rng.random() < (
            SHELL_LIKE_WHEN_LAUNDERING if laundering_linked else HARD_NEGATIVE_WHEN_BENIGN
        ):
            flags = weighted_choice(self.rng, STRUCTURE_MIXES)
            structure = "shell_like" if laundering_linked else "hard_negative"

        country = next(c for c in COUNTRIES if c.code == customer["country"])
        onboarded_at = customer["onboarded_at"]
        assert isinstance(onboarded_at, date)
        company_id = f"co-{str(customer['customer_id']).removeprefix('cus-')}"
        self._company(
            company_id,
            name=str(customer["name"]),
            legal_form=str(customer["legal_form"]),
            country=country.code,
            incorporated_before=onboarded_at,
            customer_id=str(customer["customer_id"]),
        )

        owners = [self.new_person(country) for _ in range(self.rng.choice([1, 1, 2, 3]))]
        loop_stake = self.rng.choice([25, 30, 40]) if CIRCULAR_OWNERSHIP in flags else 0
        if OFFSHORE_HOLDING in flags or self.rng.random() < BENIGN_HOLDING_COMPANY:
            abroad = self.rng.choice(OFFSHORE) if OFFSHORE_HOLDING in flags else None
            holding = self._holding_company(
                abroad.code if abroad else country.code,
                abroad.company_form if abroad else country.company_form,
                onboarded_at,
            )
            self._own(holding, owners, 100)
            self._own(company_id, [holding], 100 - loop_stake)
        else:
            self._own(company_id, owners, 100 - loop_stake)
        if loop_stake:
            self._close_a_loop(company_id, loop_stake, country, onboarded_at)

        directors = [self.rng.choice(self.nominees)] if NOMINEE_DIRECTOR in flags else owners[:2]
        for director in directors:
            self._appoint(company_id, director, "director", onboarded_at)

        self.labels.append(
            {
                "company_id": company_id,
                "customer_id": customer["customer_id"],
                "laundering_linked": laundering_linked,
                "structure": structure,
                "flags": list(flags),
                "hard_negative_story": (
                    HARD_NEGATIVE_STORIES[flags[0]] if structure == "hard_negative" else None
                ),
            }
        )

    def new_person(self, residence: Country) -> str:
        person_id = f"per-o{len(self.persons) + 1:06d}"
        person = generate_person(self.rng, self.fakers, person_id, residence, self.activity_starts)
        self.persons.append({**person, "customer_id": None})
        return person_id

    def _close_a_loop(self, company_id: str, stake: int, country: Country, before: date) -> None:
        # Part of the company is held through two holding companies, the second of which the
        # company itself part-owns: company -> first -> second -> company.
        first = self._holding_company(country.code, country.company_form, before)
        second = self._holding_company(country.code, country.company_form, before)
        self._own(company_id, [first], stake)
        self._own(first, [second], 100)
        back = self.rng.choice([20, 35, 49])
        self._own(second, [company_id], back)
        self._own(second, [self.new_person(country)], 100 - back)

    def _holding_company(self, code: str, legal_form: str, before: date) -> str:
        self.holding_companies += 1
        company_id = f"co-h{self.holding_companies:05d}"
        domestic = next((c for c in COUNTRIES if c.code == code), None)
        surname = self.fakers[domestic.locale if domestic else "en_GB"].last_name()
        self._company(
            company_id,
            name=f"{surname} Holdings {legal_form}",
            legal_form=legal_form,
            country=code,
            incorporated_before=before,
            customer_id=None,
        )
        return company_id

    def _company(
        self,
        company_id: str,
        *,
        name: str,
        legal_form: str,
        country: str,
        incorporated_before: date,
        customer_id: str | None,
    ) -> None:
        offshore = next((j for j in OFFSHORE if j.code == country), None)
        registration = offshore.registration_format if offshore else REGISTRATION_FORMATS[country]
        self.companies.append(
            {
                "company_id": company_id,
                "customer_id": customer_id,
                "name": name,
                "legal_form": legal_form,
                "country": country,
                "registration_number": registration.format(self.rng.randint(1, 999_999)),
                "incorporated_on": incorporated_before
                - timedelta(days=self.rng.randint(30, 15 * 365)),
            }
        )

    def _own(self, company_id: str, holders: list[str], percent: int) -> None:
        for holder, share in zip(
            holders, split_shares(self.rng, percent, len(holders)), strict=True
        ):
            self.shareholdings.append(
                {
                    "company_id": company_id,
                    "holder_id": holder,
                    "holder_type": "person" if holder.startswith("per-") else "company",
                    "share_percent": share,
                }
            )

    def _appoint(self, company_id: str, person_id: str, role: str, before: date) -> None:
        self.officers.append(
            {
                "company_id": company_id,
                "person_id": person_id,
                "role": role,
                "appointed_on": before - timedelta(days=self.rng.randint(0, 5 * 365)),
            }
        )

    def companies_frames(self) -> Companies:
        return Companies(
            companies=pl.DataFrame(self.companies, schema=COMPANY_SCHEMA).sort("company_id"),
            officers=pl.DataFrame(self.officers, schema=OFFICER_SCHEMA).sort(
                "company_id", "person_id"
            ),
            shareholdings=pl.DataFrame(self.shareholdings, schema=SHAREHOLDING_SCHEMA).sort(
                "company_id", "holder_id"
            ),
            persons=pl.DataFrame(self.persons, schema=PERSON_SCHEMA).sort("person_id"),
            labels=pl.DataFrame(self.labels, schema=COMPANY_LABEL_SCHEMA).sort("company_id"),
        )


def split_shares(rng: random.Random, total: int, parts: int) -> list[int]:
    """Random whole-percent shares that add up to `total`."""
    cuts = sorted(rng.sample(range(1, total), parts - 1))
    edges = [0, *cuts, total]
    return [high - low for low, high in pairwise(edges)]


COMPANY_SCHEMA = pl.Schema(
    {
        "company_id": pl.String(),
        "customer_id": pl.String(),
        "name": pl.String(),
        "legal_form": pl.String(),
        "country": pl.String(),
        "registration_number": pl.String(),
        "incorporated_on": pl.Date(),
    }
)
OFFICER_SCHEMA = pl.Schema(
    {
        "company_id": pl.String(),
        "person_id": pl.String(),
        "role": pl.String(),
        "appointed_on": pl.Date(),
    }
)
SHAREHOLDING_SCHEMA = pl.Schema(
    {
        "company_id": pl.String(),
        "holder_id": pl.String(),
        "holder_type": pl.String(),
        "share_percent": pl.Int64(),
    }
)
COMPANY_LABEL_SCHEMA = pl.Schema(
    {
        "company_id": pl.String(),
        "customer_id": pl.String(),
        "laundering_linked": pl.Boolean(),
        "structure": pl.String(),
        "flags": pl.List(pl.String),
        "hard_negative_story": pl.String(),
    }
)

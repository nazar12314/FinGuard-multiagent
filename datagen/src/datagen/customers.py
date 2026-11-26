import math
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
from faker import Faker

from datagen.amlworld import AmlworldFormatError, account_id, bank_id
from finguard.modules.screening.domain.romanisation import romanize_ukrainian

ACCOUNTS_HEADER = "Bank Name,Bank ID,Account Number,Entity ID,Entity Name"

CustomerType = Literal["sole_trader", "company"]

ENTITY_TYPES: dict[str, CustomerType] = {
    "Individual": "sole_trader",
    "Sole Proprietorship": "sole_trader",
    "Partnership": "company",
    "Corporation": "company",
    "Country": "company",
    "Direct": "company",
}

# Share of laundering-linked customers whose profile doesn't fit their activity, and of benign
# customers whose profile looks just as odd for an innocent reason, so a profile alone never
# gives the label away.
INCONSISTENT_WHEN_LAUNDERING = 0.6
HARD_NEGATIVE_WHEN_BENIGN = 0.1

VOLUME_BELOW_ACTIVITY = "declared_volume_far_below_activity"
RECENTLY_ONBOARDED = "onboarded_shortly_before_activity"
VAGUE_PURPOSE = "vague_account_purpose"
ANOMALY_MIXES: list[tuple[tuple[str, ...], float]] = [
    ((VOLUME_BELOW_ACTIVITY,), 0.25),
    ((RECENTLY_ONBOARDED,), 0.2),
    ((VAGUE_PURPOSE,), 0.15),
    ((VOLUME_BELOW_ACTIVITY, RECENTLY_ONBOARDED), 0.15),
    ((VOLUME_BELOW_ACTIVITY, VAGUE_PURPOSE), 0.1),
    ((RECENTLY_ONBOARDED, VAGUE_PURPOSE), 0.05),
    ((VOLUME_BELOW_ACTIVITY, RECENTLY_ONBOARDED, VAGUE_PURPOSE), 0.1),
]
# The innocent explanation behind a hard negative, by its first anomaly.
HARD_NEGATIVE_STORIES = {
    VOLUME_BELOW_ACTIVITY: "seasonal peak well above the declared average",
    RECENTLY_ONBOARDED: "newly founded business growing fast",
    VAGUE_PURPOSE: "owner runs everything through one account",
}

NORMAL_PURPOSES = [
    "receiving customer payments",
    "paying suppliers",
    "payroll and operating costs",
    "cross-border trade settlement",
]
VAGUE_PURPOSES = ["general purposes", "personal and business use", "investments"]


@dataclass(frozen=True)
class Industry:
    name: str
    trade_name: str
    risk: int
    sole_traders: bool = True
    companies: bool = True


INDUSTRIES = [
    Industry("software development", "Software", 0),
    Industry("IT consulting", "Consulting", 0),
    Industry("marketing agency", "Marketing", 0),
    Industry("accounting services", "Accounting", 0),
    Industry("translation services", "Translations", 0, companies=False),
    Industry("e-commerce retail", "Online Store", 1),
    Industry("construction", "Construction", 1),
    Industry("restaurant", "Kitchen", 1),
    Industry("beauty salon", "Beauty", 1, companies=False),
    Industry("taxi and ride hailing", "Transport", 1, companies=False),
    Industry("freight forwarding", "Logistics", 1, sole_traders=False),
    Industry("car wash", "Car Care", 2, companies=False),
    Industry("used car dealership", "Motors", 2, sole_traders=False),
    Industry("wholesale electronics", "Electronics Trading", 2, sole_traders=False),
    Industry("import and export trading", "Trading", 2, sole_traders=False),
    Industry("real estate agency", "Properties", 2, sole_traders=False),
]


@dataclass(frozen=True)
class Country:
    code: str
    locale: str
    weight: float
    partnership_form: str
    company_form: str


# Where customers are registered: a neobank passported across the EU.
COUNTRIES = [
    Country("DE", "de_DE", 0.30, "GbR", "GmbH"),
    Country("PL", "pl_PL", 0.20, "sp.j.", "sp. z o.o."),
    Country("NL", "nl_NL", 0.12, "VOF", "B.V."),
    Country("RO", "ro_RO", 0.10, "SNC", "SRL"),
    Country("IE", "en_IE", 0.10, "Partnership", "Ltd"),
    Country("FR", "fr_FR", 0.10, "SNC", "SARL"),
    Country("ES", "es_ES", 0.08, "S.C.", "S.L."),
]
# Most sole traders are nationals of their country of residence; the rest come from these.
FOREIGN_NATIONALITIES = [("UA", "uk_UA", 0.5), ("GB", "en_GB", 0.2), ("IT", "it_IT", 0.15)]
FOREIGN_NATIONAL_SHARE = 0.25

EARLIEST_ONBOARDING = date(2015, 1, 1)
RECENT_ONBOARDING_DAYS = 90


@dataclass(frozen=True)
class Customers:
    customers: pl.DataFrame
    kyc_profiles: pl.DataFrame
    persons: pl.DataFrame
    labels: pl.DataFrame


def accounts_csv(raw_dir: Path, variant: str) -> Path:
    return raw_dir / f"{variant}_accounts.csv"


def read_account_holders(path: Path, accounts: pl.DataFrame) -> pl.DataFrame:
    """The AMLworld entity behind each of our bank's accounts, as its customer."""
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found; download it from the IBM AMLworld dataset on Kaggle"
        )
    with path.open() as csv_file:
        header = csv_file.readline().strip()
    if header != ACCOUNTS_HEADER:
        raise AmlworldFormatError(f"{path.name} has an unexpected header: {header}")
    entities = pl.read_csv(path, infer_schema=False).select(
        account_id=account_id(bank_id(pl.col("Bank ID")), pl.col("Account Number")),
        entity_id="Entity ID",
        entity_type=pl.col("Entity Name").str.replace(r" #\d+$", ""),
    )
    if unknown := sorted(set(entities.get_column("entity_type")) - ENTITY_TYPES.keys()):
        raise AmlworldFormatError(f"unknown entity types in {path.name}: {unknown}")
    ours = accounts.filter("held_at_our_bank").select("account_id")
    holders = ours.join(entities, on="account_id", how="left")
    if missing := holders.filter(pl.col("entity_id").is_null()).height:
        raise AmlworldFormatError(f"{missing} of our accounts have no entity in {path.name}")
    return holders.with_columns(
        customer_id=pl.format("cus-{}", "entity_id"),
        customer_type=pl.col("entity_type").replace_strict(ENTITY_TYPES),
    ).sort("account_id")


def monthly_volumes(transactions: pl.DataFrame, holders: pl.DataFrame) -> pl.DataFrame:
    """Each customer's average monthly flow in EUR, leaving out moves between its own accounts."""
    span = transactions.select(pl.col("booked_at").max() - pl.col("booked_at").min()).item()
    days = max(span.total_seconds() / 86400, 1.0)
    owner = holders.select("account_id", "customer_id")
    flows = (
        transactions.join(
            owner.rename({"account_id": "from_account_id", "customer_id": "payer"}),
            on="from_account_id",
            how="left",
        )
        .join(
            owner.rename({"account_id": "to_account_id", "customer_id": "payee"}),
            on="to_account_id",
            how="left",
        )
        .filter(
            pl.col("payer").is_null()
            | pl.col("payee").is_null()
            | (pl.col("payer") != pl.col("payee"))
        )
    )
    sides = pl.concat(
        [
            flows.select(customer_id="payer", amount_eur="amount_paid_eur"),
            flows.select(customer_id="payee", amount_eur="amount_received_eur"),
        ]
    ).drop_nulls("customer_id")
    return (
        holders.select("customer_id")
        .unique()
        .join(
            sides.group_by("customer_id").agg(total=pl.col("amount_eur").sum().cast(pl.Float64)),
            on="customer_id",
            how="left",
        )
        .select("customer_id", monthly_volume_eur=pl.col("total").fill_null(0) / 2 * 30 / days)
        .sort("customer_id")
    )


def generate_customers(
    holders: pl.DataFrame,
    volumes: pl.DataFrame,
    account_labels: pl.DataFrame,
    activity_starts: date,
    seed: int,
) -> Customers:
    rng = random.Random(f"{seed}:customers")
    fakers = locale_fakers(seed, "customers")
    laundering = set(
        holders.join(account_labels, on="account_id")
        .filter(pl.col("laundering_transaction_count") > 0)
        .get_column("customer_id")
    )
    customers = (
        holders.select("customer_id", "entity_id", "entity_type", "customer_type")
        .unique()
        .join(volumes, on="customer_id")
        .sort("customer_id")
    )

    customer_rows, profile_rows, person_rows, label_rows = [], [], [], []
    for customer in customers.iter_rows(named=True):
        laundering_linked = customer["customer_id"] in laundering
        anomalies: tuple[str, ...] = ()
        profile = "consistent"
        if rng.random() < (
            INCONSISTENT_WHEN_LAUNDERING if laundering_linked else HARD_NEGATIVE_WHEN_BENIGN
        ):
            anomalies = weighted_choice(rng, ANOMALY_MIXES)
            profile = "inconsistent" if laundering_linked else "hard_negative"

        country = weighted_choice(rng, [(country, country.weight) for country in COUNTRIES])
        sole_trader = customer["customer_type"] == "sole_trader"
        industry = rng.choice(
            [i for i in INDUSTRIES if (i.sole_traders if sole_trader else i.companies)]
        )
        form = None
        if sole_trader:
            person_id = f"per-{customer['entity_id']}"
            person = generate_person(rng, fakers, person_id, country, activity_starts)
            person_rows.append({**person, "customer_id": customer["customer_id"]})
            name = f"{person['full_name_latin']} {industry.trade_name}"
        else:
            form = (
                country.partnership_form
                if customer["entity_type"] == "Partnership"
                else country.company_form
            )
            name = f"{fakers[country.locale].last_name()} {industry.trade_name} {form}"

        onboarded_at = _onboarding_date(rng, activity_starts, RECENTLY_ONBOARDED in anomalies)
        expected_monthly = _declared_monthly_volume(
            rng, customer["monthly_volume_eur"], VOLUME_BELOW_ACTIVITY in anomalies
        )
        purpose = rng.choice(VAGUE_PURPOSES if VAGUE_PURPOSE in anomalies else NORMAL_PURPOSES)
        customer_rows.append(
            {
                "customer_id": customer["customer_id"],
                "customer_type": customer["customer_type"],
                "name": name,
                "country": country.code,
                "industry": industry.name,
                "legal_form": form,
                "onboarded_at": onboarded_at,
            }
        )
        profile_rows.append(
            {
                "customer_id": customer["customer_id"],
                "expected_monthly_volume_eur": expected_monthly,
                "declared_annual_turnover_eur": _round_like_a_person(
                    expected_monthly * 12 * rng.lognormvariate(0, 0.15)
                ),
                "account_purpose": purpose,
                "risk_rating": risk_rating(
                    industry, onboarded_at, activity_starts, expected_monthly, purpose
                ),
            }
        )
        label_rows.append(
            {
                "customer_id": customer["customer_id"],
                "entity_id": customer["entity_id"],
                "laundering_linked": laundering_linked,
                "profile": profile,
                "anomalies": list(anomalies),
                "hard_negative_story": (
                    HARD_NEGATIVE_STORIES[anomalies[0]] if profile == "hard_negative" else None
                ),
            }
        )

    return Customers(
        customers=pl.DataFrame(customer_rows, schema=CUSTOMER_SCHEMA),
        kyc_profiles=pl.DataFrame(profile_rows, schema=KYC_PROFILE_SCHEMA),
        persons=pl.DataFrame(person_rows, schema=PERSON_SCHEMA),
        labels=pl.DataFrame(label_rows, schema=CUSTOMER_LABEL_SCHEMA),
    )


CUSTOMER_SCHEMA = pl.Schema(
    {
        "customer_id": pl.String,
        "customer_type": pl.String,
        "name": pl.String,
        "country": pl.String,
        "industry": pl.String,
        "legal_form": pl.String,
        "onboarded_at": pl.Date,
    }
)
KYC_PROFILE_SCHEMA = pl.Schema(
    {
        "customer_id": pl.String,
        "expected_monthly_volume_eur": pl.Int64,
        "declared_annual_turnover_eur": pl.Int64,
        "account_purpose": pl.String,
        "risk_rating": pl.String,
    }
)
PERSON_SCHEMA = pl.Schema(
    {
        "person_id": pl.String,
        "customer_id": pl.String,
        "full_name": pl.String,
        "full_name_latin": pl.String,
        "birth_date": pl.Date,
        "nationality": pl.String,
    }
)
CUSTOMER_LABEL_SCHEMA = pl.Schema(
    {
        "customer_id": pl.String(),
        "entity_id": pl.String(),
        "laundering_linked": pl.Boolean(),
        "profile": pl.String(),
        "anomalies": pl.List(pl.String),
        "hard_negative_story": pl.String(),
    }
)


def locale_fakers(seed: int, stream: str) -> dict[str, Faker]:
    """A seeded Faker per locale; each generator step uses its own stream, so adding one step
    doesn't change what the others generate."""
    locales = {country.locale for country in COUNTRIES} | {
        locale for _, locale, _ in FOREIGN_NATIONALITIES
    }
    fakers = {}
    for locale in sorted(locales):
        faker = Faker(locale)
        faker.seed_instance(f"{seed}:{stream}:{locale}")
        if locale == "it_IT":
            # Faker builds the Italian city list from a set, so its order, and every city()
            # drawn from it, changes with Python's hash seed.
            address = next(p for p in faker.get_providers() if hasattr(p, "cities"))
            address.cities = sorted(address.cities)
        fakers[locale] = faker
    return fakers


def generate_person(
    rng: random.Random,
    fakers: dict[str, Faker],
    person_id: str,
    residence: Country,
    activity_starts: date,
) -> dict[str, object]:
    nationality, locale = residence.code, residence.locale
    if rng.random() < FOREIGN_NATIONAL_SHARE:
        nationality, locale = weighted_choice(
            rng, [((code, loc), weight) for code, loc, weight in FOREIGN_NATIONALITIES]
        )
    faker = fakers[locale]
    first_name = faker.first_name_female() if rng.random() < 0.5 else faker.first_name_male()
    full_name = f"{first_name} {faker.last_name()}"
    age_days = rng.randint(21 * 365, 70 * 365)
    return {
        "person_id": person_id,
        "full_name": full_name,
        "full_name_latin": romanize_ukrainian(full_name) if locale == "uk_UA" else full_name,
        "birth_date": activity_starts - timedelta(days=age_days),
        "nationality": nationality,
    }


def _onboarding_date(rng: random.Random, activity_starts: date, recent: bool) -> date:
    recent_from = activity_starts - timedelta(days=RECENT_ONBOARDING_DAYS)
    if recent:
        earliest, latest = recent_from, activity_starts - timedelta(days=7)
    else:
        earliest, latest = EARLIEST_ONBOARDING, recent_from - timedelta(days=1)
    return earliest + timedelta(days=rng.randint(0, (latest - earliest).days))


def _declared_monthly_volume(rng: random.Random, observed: float, far_below: bool) -> int:
    # Accounts that only move money between their own accounts still declare something.
    observed = max(observed, rng.uniform(2_000, 20_000))
    factor = rng.uniform(0.03, 0.25) if far_below else rng.lognormvariate(0, 0.25)
    return _round_like_a_person(observed * factor)


def _round_like_a_person(amount: float) -> int:
    # People declare round figures: two significant digits.
    if amount < 100:
        return 100
    magnitude = 10 ** (int(math.log10(amount)) - 1)
    return int(round(amount / magnitude) * magnitude)


def risk_rating(
    industry: Industry,
    onboarded_at: date,
    activity_starts: date,
    expected_monthly: int,
    purpose: str,
) -> str:
    # The onboarding rule sees only what the customer declared, never the label.
    score = industry.risk
    score += (activity_starts - onboarded_at).days <= 180
    score += expected_monthly >= 1_000_000
    score += purpose in VAGUE_PURPOSES
    return "low" if score <= 1 else "medium" if score == 2 else "high"


def weighted_choice[T](rng: random.Random, choices: Sequence[tuple[T, float]]) -> T:
    [choice] = rng.choices([value for value, _ in choices], [weight for _, weight in choices])
    return choice

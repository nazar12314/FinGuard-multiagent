import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import polars as pl

from datagen.customers import FOREIGN_NATIONALITIES, locale_fakers
from datagen.media_corpus import IRRELEVANT_NAMESAKE, NOT_ADVERSE, RELEVANT_ADVERSE
from datagen.people import customers_of_people, laundering_linked_people

# Adverse coverage clusters around laundering, but some honest customers get accused too,
# namesakes' crimes land on anyone, and plenty of coverage is harmless.
RELEVANT_WHEN_LAUNDERING = 0.25
RELEVANT_WHEN_BENIGN = 0.01
NAMESAKE = 0.03
NOT_ADVERSE_COVERAGE = 0.05

DISCLAIMER = "Fictional article generated for testing; the outlet, people and events are invented."
OTHER_OCCUPATIONS = ["a bus driver", "a nurse", "a warehouse worker", "a teacher", "a chef"]


@dataclass(frozen=True)
class Media:
    articles: pl.DataFrame
    labels: pl.DataFrame


@dataclass(frozen=True)
class _Subject:
    subject_type: str
    subject_id: str
    laundering_linked: bool
    values: dict[str, Any]


def assign_media(
    world: dict[str, pl.DataFrame],
    corpus: list[dict[str, Any]],
    activity_starts: date,
    seed: int,
) -> Media:
    rng = random.Random(f"{seed}:media")
    fakers = locale_fakers(seed, "media")
    templates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for article in corpus:
        templates[(article["label"], article["subject_kind"])].append(article)

    articles: list[dict[str, Any]] = []
    labels: list[dict[str, Any]] = []
    for subject in _subjects(world):
        kind = "person" if subject.subject_type == "person" else "company"
        relevant = RELEVANT_WHEN_LAUNDERING if subject.laundering_linked else RELEVANT_WHEN_BENIGN
        coverage = [
            (RELEVANT_ADVERSE, relevant),
            (IRRELEVANT_NAMESAKE, NAMESAKE if kind == "person" else 0.0),
            (NOT_ADVERSE, NOT_ADVERSE_COVERAGE),
        ]
        for label, rate in coverage:
            pool = templates[(label, kind)]
            if not pool or rng.random() >= rate:
                continue
            template = rng.choice(pool)
            values = {**subject.values}
            # Coverage from the last three years, and never of a company before it existed.
            earliest = max(
                activity_starts - timedelta(days=3 * 365),
                values.pop("incorporated_on", date.min),
            )
            latest = activity_starts - timedelta(days=30)
            published_on = earliest + timedelta(
                days=rng.randint(0, max(0, (latest - earliest).days))
            )
            if "birth_date" in values:
                values["age"] = (published_on - values.pop("birth_date")).days // 365
            if label == IRRELEVANT_NAMESAKE:
                _, locale, _ = rng.choice(FOREIGN_NATIONALITIES)
                values["other_city"] = fakers[locale].city()
                values["other_age"] = max(
                    19, values["age"] + rng.choice([-1, 1]) * rng.randint(9, 25)
                )
                values["other_occupation"] = rng.choice(OTHER_OCCUPATIONS)
            article_id = f"art-{len(articles) + 1:05d}"
            articles.append(
                {
                    "article_id": article_id,
                    "outlet": template["outlet"],
                    "published_on": published_on,
                    "headline": template["headline"].format_map(values),
                    "body": template["body"].format_map(values),
                    "disclaimer": DISCLAIMER,
                }
            )
            labels.append(
                {
                    "article_id": article_id,
                    "subject_type": subject.subject_type,
                    "subject_id": subject.subject_id,
                    "label": label,
                    "story": template["story"],
                }
            )
    return Media(
        articles=pl.DataFrame(articles, schema=ARTICLE_SCHEMA),
        labels=pl.DataFrame(labels, schema=MEDIA_LABEL_SCHEMA),
    )


def _subjects(world: dict[str, pl.DataFrame]) -> list[_Subject]:
    """Sole traders, the officers of company customers, and the companies. A person is
    laundering-linked if any customer they stand behind is; an officer of several companies
    is written up with the first one's city and industry."""
    customers = {row["customer_id"]: row for row in world["customers.parquet"].rows(named=True)}
    linked = dict(
        world["ground_truth/customer_labels.parquet"]
        .select("customer_id", "laundering_linked")
        .iter_rows()
    )
    addresses = {r["address_id"]: r for r in world["addresses.parquet"].rows(named=True)}
    city = {
        (row["customer_id"], row["role"]): addresses[row["address_id"]]["city"]
        for row in world["customer_addresses.parquet"].rows(named=True)
    }
    persons = {row["person_id"]: row for row in world["persons.parquet"].rows(named=True)}
    company_customer = dict(
        world["companies.parquet"].select("company_id", "customer_id").iter_rows()
    )

    people: dict[str, str] = {}
    for person in persons.values():
        if person["customer_id"]:
            people[person["person_id"]] = person["customer_id"]
    for company_id, person_id in (
        world["officers.parquet"]
        .sort("company_id", "person_id")
        .select("company_id", "person_id")
        .iter_rows()
    ):
        customer_id = company_customer.get(company_id)
        if customer_id and person_id not in people:
            people[person_id] = customer_id

    linked_people = laundering_linked_people(
        customers_of_people(
            world["persons.parquet"],
            world["companies.parquet"],
            world["officers.parquet"],
            world["shareholdings.parquet"],
        ),
        world["ground_truth/customer_labels.parquet"],
    )
    incorporated = dict(
        world["companies.parquet"].select("customer_id", "incorporated_on").drop_nulls().iter_rows()
    )

    subjects = []
    for person_id, customer_id in sorted(people.items()):
        customer = customers[customer_id]
        role = "owner_home" if customer["customer_type"] == "sole_trader" else "registered"
        subjects.append(
            _Subject(
                "person",
                person_id,
                person_id in linked_people,
                {
                    "name": persons[person_id]["full_name_latin"],
                    "city": city[(customer_id, role)],
                    "industry": customer["industry"],
                    "birth_date": persons[person_id]["birth_date"],
                },
            )
        )
    for customer_id, customer in sorted(customers.items()):
        if customer["customer_type"] == "company":
            subjects.append(
                _Subject(
                    "company",
                    customer_id,
                    linked[customer_id],
                    {
                        "company": customer["name"],
                        "city": city[(customer_id, "registered")],
                        "industry": customer["industry"],
                        "incorporated_on": incorporated[customer_id],
                    },
                )
            )
    return subjects


ARTICLE_SCHEMA = pl.Schema(
    {
        "article_id": pl.String(),
        "outlet": pl.String(),
        "published_on": pl.Date(),
        "headline": pl.String(),
        "body": pl.String(),
        "disclaimer": pl.String(),
    }
)
MEDIA_LABEL_SCHEMA = pl.Schema(
    {
        "article_id": pl.String(),
        "subject_type": pl.String(),
        "subject_id": pl.String(),
        "label": pl.String(),
        "story": pl.String(),
    }
)

"""The data-quality report every build writes into its world.

Checks are the invariants the rest of FinGuard relies on (keys, references, ownership that adds
up, labels that agree with each other); a world that fails one is not published. Signals measure
how much each planted, noisy red flag says about the laundering label, which should be something
but never everything.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pypdfium2 as pdfium

from datagen.people import customers_of_people

EXAMPLES = 5

KEYS = {
    "transactions": ["transaction_id"],
    "accounts": ["account_id"],
    "customers": ["customer_id"],
    "kyc_profiles": ["customer_id"],
    "persons": ["person_id"],
    "companies": ["company_id"],
    "officers": ["company_id", "person_id"],
    "shareholdings": ["company_id", "holder_id"],
    "addresses": ["address_id"],
    "customer_addresses": ["customer_id", "role"],
    "devices": ["device_id"],
    "customer_devices": ["customer_id", "device_id"],
    "documents": ["document_id"],
    "media/articles": ["article_id"],
    "ground_truth/transaction_labels": ["transaction_id"],
    "ground_truth/laundering_attempts": ["attempt_id"],
    "ground_truth/account_labels": ["account_id"],
    "ground_truth/customer_labels": ["customer_id"],
    "ground_truth/company_labels": ["company_id"],
    "ground_truth/shared_attribute_labels": ["attribute", "attribute_id"],
    "ground_truth/screening_labels": ["subject_type", "subject_id"],
    "ground_truth/media_labels": ["article_id"],
    "ground_truth/document_labels": ["document_id"],
}

# (table, column, referenced table, referenced column). Read "every non-null value of column
# appears in the referenced column". Pointing one the other way round says every row has one.
REFERENCES = [
    ("transactions", "from_account_id", "accounts", "account_id"),
    ("transactions", "to_account_id", "accounts", "account_id"),
    ("accounts", "customer_id", "customers", "customer_id"),
    ("kyc_profiles", "customer_id", "customers", "customer_id"),
    ("customers", "customer_id", "kyc_profiles", "customer_id"),
    ("persons", "customer_id", "customers", "customer_id"),
    ("companies", "customer_id", "customers", "customer_id"),
    ("officers", "company_id", "companies", "company_id"),
    ("officers", "person_id", "persons", "person_id"),
    ("shareholdings", "company_id", "companies", "company_id"),
    ("customer_addresses", "customer_id", "customers", "customer_id"),
    ("customer_addresses", "address_id", "addresses", "address_id"),
    ("customer_devices", "customer_id", "customers", "customer_id"),
    ("customer_devices", "device_id", "devices", "device_id"),
    ("documents", "customer_id", "customers", "customer_id"),
    ("ground_truth/transaction_labels", "transaction_id", "transactions", "transaction_id"),
    ("transactions", "transaction_id", "ground_truth/transaction_labels", "transaction_id"),
    (
        "ground_truth/transaction_labels",
        "attempt_id",
        "ground_truth/laundering_attempts",
        "attempt_id",
    ),
    ("ground_truth/account_labels", "account_id", "accounts", "account_id"),
    ("ground_truth/customer_labels", "customer_id", "customers", "customer_id"),
    ("customers", "customer_id", "ground_truth/customer_labels", "customer_id"),
    ("ground_truth/company_labels", "company_id", "companies", "company_id"),
    ("ground_truth/company_labels", "customer_id", "customers", "customer_id"),
    ("ground_truth/media_labels", "article_id", "media/articles", "article_id"),
    ("media/articles", "article_id", "ground_truth/media_labels", "article_id"),
    ("ground_truth/document_labels", "document_id", "documents", "document_id"),
    ("documents", "document_id", "ground_truth/document_labels", "document_id"),
]

# Labels that point at a person or a company, by the type column that says which.
POLYMORPHIC_REFERENCES = [
    ("shareholdings", "holder_type", "holder_id", {"person": "persons", "company": "companies"}),
    (
        "ground_truth/shared_attribute_labels",
        "attribute",
        "attribute_id",
        {"address": "addresses", "device": "devices"},
    ),
    (
        "ground_truth/screening_labels",
        "subject_type",
        "subject_id",
        {"person": "persons", "company": "customers"},
    ),
    (
        "ground_truth/media_labels",
        "subject_type",
        "subject_id",
        {"person": "persons", "company": "customers"},
    ),
]


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class TableProfile:
    rows: int
    null_rates: dict[str, float]


@dataclass(frozen=True)
class Check:
    name: str
    failures: int
    examples: list[str]


@dataclass(frozen=True)
class Signal:
    name: str
    population: str
    population_size: int
    laundering_linked: int
    flagged: int
    flagged_laundering_linked: int
    # What a signal that says nothing would catch by chance: flagged times the base rate.
    expected_by_chance: float
    # Share of flagged customers who are laundering-linked, of laundering-linked customers who
    # are flagged, and precision over the population's base rate (1 means it says nothing).
    precision: float | None
    recall: float | None
    lift: float | None


@dataclass(frozen=True)
class BookingDay:
    day: str
    transactions: int
    laundering: int


@dataclass(frozen=True)
class QualityReport:
    world: str
    tables: dict[str, TableProfile]
    checks: list[Check]
    signals: list[Signal]
    # AMLworld's laundering doesn't spread evenly over time, which time-based splits and
    # features have to allow for.
    booking_days: list[BookingDay]

    @property
    def failed_checks(self) -> list[Check]:
        return [check for check in self.checks if check.failures]

    def to_json(self) -> dict[str, Any]:
        return {"passed": not self.failed_checks, **asdict(self)}


def quality_report(world: Path, files: dict[str, pl.DataFrame]) -> QualityReport:
    """Profile and check the world's frames; `world` holds the documents they point at."""
    tables = {name.removesuffix(".parquet"): frame for name, frame in files.items()}
    return QualityReport(
        world=world.name.removeprefix(".").removesuffix(".building"),
        tables={name: _profile(frame) for name, frame in sorted(tables.items())},
        checks=_checks(tables, world),
        signals=_signals(tables),
        booking_days=_booking_days(tables),
    )


def write_quality_report(report: QualityReport, world: Path) -> None:
    (world / "quality_report.json").write_text(json.dumps(report.to_json(), indent=2) + "\n")
    (world / "quality_report.md").write_text(render_markdown(report))


def _profile(frame: pl.DataFrame) -> TableProfile:
    nulls = frame.null_count().row(0, named=True) if frame.height else {}
    return TableProfile(
        rows=frame.height,
        null_rates={column: round(count / frame.height, 4) for column, count in nulls.items()},
    )


def _checks(tables: dict[str, pl.DataFrame], world: Path) -> list[Check]:
    checks = [_unique(tables, name, columns) for name, columns in KEYS.items()]
    checks += [_references(tables, *reference) for reference in REFERENCES]
    for table, type_column, id_column, targets in POLYMORPHIC_REFERENCES:
        checks += _polymorphic_references(tables, table, type_column, id_column, targets)
    checks += [check(tables) for check in DOMAIN_CHECKS]
    checks += [check(tables, world) for check in DOCUMENT_CHECKS]
    return checks


def _unique(tables: dict[str, pl.DataFrame], table: str, columns: list[str]) -> Check:
    frame = tables[table]
    duplicated = frame.filter(pl.struct(columns).is_duplicated())
    return _check(f"{table}: {', '.join(columns)} is unique", _keys(duplicated, columns))


def _references(
    tables: dict[str, pl.DataFrame], table: str, column: str, target: str, target_column: str
) -> Check:
    missing = (
        tables[table]
        .filter(pl.col(column).is_not_null())
        .join(tables[target].select(pl.col(target_column).alias(column)), on=column, how="anti")
    )
    return _check(f"{table}.{column} is in {target}.{target_column}", missing.get_column(column))


def _polymorphic_references(
    tables: dict[str, pl.DataFrame],
    table: str,
    type_column: str,
    id_column: str,
    targets: dict[str, str],
) -> list[Check]:
    frame = tables[table]
    unknown = frame.filter(~pl.col(type_column).is_in(list(targets)))
    checks = [_check(f"{table}.{type_column} is one of {', '.join(targets)}", unknown[type_column])]
    for kind, target in targets.items():
        rows = frame.filter(pl.col(type_column) == kind)
        missing = rows.join(
            tables[target].select(pl.col(KEYS[target][0]).alias(id_column)),
            on=id_column,
            how="anti",
        )
        name = f"{table}.{id_column} of a {kind} is in {target}"
        checks.append(_check(name, missing.get_column(id_column)))
    return checks


def _companies_owned_in_full(tables: dict[str, pl.DataFrame]) -> Check:
    totals = tables["shareholdings"].group_by("company_id").agg(total=pl.col("share_percent").sum())
    wrong = (
        tables["companies"]
        .join(totals, on="company_id", how="left")
        .filter(pl.col("total").fill_null(0) != 100)
    )
    return _check("every company's shares add up to 100%", wrong["company_id"])


def _customer_companies_have_a_director(tables: dict[str, pl.DataFrame]) -> Check:
    directors = tables["officers"].filter(pl.col("role") == "director")
    without = (
        tables["companies"]
        .filter(pl.col("customer_id").is_not_null())
        .join(directors, on="company_id", how="anti")
    )
    return _check("every company customer has a director", without["company_id"])


def _accounts_at_our_bank_have_a_customer(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = tables["accounts"].filter(
        pl.col("held_at_our_bank") != pl.col("customer_id").is_not_null()
    )
    return _check("exactly the accounts at our bank have a customer", wrong["account_id"])


def _laundering_linked_matches_transactions(tables: dict[str, pl.DataFrame]) -> Check:
    linked = (
        tables["accounts"]
        .join(tables["ground_truth/account_labels"], on="account_id")
        .filter(pl.col("laundering_transaction_count") > 0)
        .get_column("customer_id")
        .drop_nulls()
        .implode()
    )
    wrong = tables["ground_truth/customer_labels"].filter(
        pl.col("laundering_linked") != pl.col("customer_id").is_in(linked)
    )
    return _check(
        "a customer is laundering-linked exactly when one of its accounts has a laundering "
        "transaction",
        wrong["customer_id"],
    )


def _company_labels_agree_with_customer_labels(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = (
        tables["ground_truth/company_labels"]
        .join(
            tables["ground_truth/customer_labels"].select(
                "customer_id", customer="laundering_linked"
            ),
            on="customer_id",
        )
        .filter(pl.col("laundering_linked") != pl.col("customer"))
    )
    return _check("company labels agree with their customer's label", wrong["company_id"])


def _attempt_transactions_are_laundering(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = tables["ground_truth/transaction_labels"].filter(
        pl.col("attempt_id").is_not_null() & ~pl.col("is_laundering")
    )
    return _check("every transaction in an attempt is laundering", wrong["transaction_id"])


def _attempt_sizes_match_their_transactions(tables: dict[str, pl.DataFrame]) -> Check:
    seen = (
        tables["ground_truth/transaction_labels"]
        .filter(pl.col("attempt_id").is_not_null())
        .group_by("attempt_id")
        .agg(seen=pl.len())
    )
    wrong = (
        tables["ground_truth/laundering_attempts"]
        .join(seen, on="attempt_id", how="full", coalesce=True)
        .filter(
            (pl.col("visible_transaction_count").fill_null(0) != pl.col("seen").fill_null(0))
            | (pl.col("visible_transaction_count") > pl.col("transaction_count"))
        )
    )
    return _check(
        "each attempt's visible transaction count matches its labelled transactions",
        wrong["attempt_id"],
    )


def _amounts_are_positive(tables: dict[str, pl.DataFrame]) -> Check:
    original = ["amount_paid", "amount_received"]
    # AMLworld has sub-cent cheques (0.02 MXN), which are worth €0.00 once converted.
    eur = ["amount_paid_eur", "amount_received_eur"]
    wrong = tables["transactions"].filter(
        pl.any_horizontal(pl.col(original).is_null() | (pl.col(original) <= 0))
        | pl.any_horizontal(pl.col(eur).is_null() | (pl.col(eur) < 0))
    )
    return _check(
        "every transaction has positive amounts, and none is negative in EUR",
        wrong["transaction_id"],
    )


def _customers_onboarded_before_their_activity(tables: dict[str, pl.DataFrame]) -> Check:
    transactions = tables["transactions"]
    first_booking = (
        pl.concat(
            [
                transactions.select("booked_at", account_id="from_account_id"),
                transactions.select("booked_at", account_id="to_account_id"),
            ]
        )
        .join(tables["accounts"].select("account_id", "customer_id"), on="account_id")
        .group_by("customer_id")
        .agg(first=pl.col("booked_at").min().dt.date())
    )
    wrong = (
        tables["customers"]
        .join(first_booking, on="customer_id")
        .filter(pl.col("onboarded_at") > pl.col("first"))
    )
    return _check("every customer is onboarded before its first transaction", wrong["customer_id"])


def _officers_appointed_while_the_company_existed(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = (
        tables["officers"]
        .join(
            tables["companies"].select("company_id", "customer_id", "incorporated_on"),
            on="company_id",
        )
        .join(
            tables["customers"].select("customer_id", "onboarded_at"), on="customer_id", how="left"
        )
        .filter(
            (pl.col("appointed_on") < pl.col("incorporated_on"))
            | (pl.col("appointed_on") > pl.col("onboarded_at"))
        )
    )
    return _check(
        "every director is appointed after incorporation and before the company joined us",
        _keys(wrong, ["company_id", "person_id"]),
    )


def _adults_when_they_sign(tables: dict[str, pl.DataFrame]) -> Check:
    persons = tables["persons"]
    adult_on = pl.col("birth_date").dt.offset_by("18y")
    directors = tables["officers"].join(persons.select("person_id", "birth_date"), on="person_id")
    sole_traders = persons.filter(pl.col("customer_id").is_not_null()).join(
        tables["customers"].select("customer_id", "onboarded_at"), on="customer_id"
    )
    minors = pl.concat(
        [
            directors.filter(pl.col("appointed_on") < adult_on).select("person_id"),
            sole_traders.filter(pl.col("onboarded_at") < adult_on).select("person_id"),
        ]
    )
    return _check(
        "every director is an adult when appointed, and every sole trader when onboarded",
        minors["person_id"],
    )


def _registered_in_their_own_country(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = (
        tables["customer_addresses"]
        .filter(pl.col("role") == "registered")
        .join(tables["addresses"].select("address_id", address_country="country"), on="address_id")
        .join(tables["customers"].select("customer_id", "country"), on="customer_id")
        .filter(pl.col("address_country") != pl.col("country"))
    )
    return _check("every customer's registered address is in its own country", wrong["customer_id"])


def _customers_have_their_records(tables: dict[str, pl.DataFrame]) -> Check:
    customers = tables["customers"]
    companies = customers.filter(pl.col("customer_type") == "company").join(
        tables["companies"], on="customer_id", how="anti"
    )
    sole_traders = customers.filter(pl.col("customer_type") == "sole_trader").join(
        tables["persons"].filter(pl.col("customer_id").is_not_null()), on="customer_id", how="anti"
    )
    return _check(
        "every company customer has a company record, and every sole trader a person",
        pl.concat([companies["customer_id"], sole_traders["customer_id"]]),
    )


def _shared_attributes_are_explained(tables: dict[str, pl.DataFrame]) -> Check:
    shared = pl.concat(
        [
            tables["customer_addresses"]
            .group_by(attribute_id="address_id")
            .agg(users=pl.col("customer_id").n_unique()),
            tables["customer_devices"]
            .group_by(attribute_id="device_id")
            .agg(users=pl.col("customer_id").n_unique()),
        ]
    ).filter(pl.col("users") > 1)
    unexplained = shared.join(
        tables["ground_truth/shared_attribute_labels"], on="attribute_id", how="anti"
    )
    return _check(
        "every address or device several customers use has a labelled reason",
        unexplained["attribute_id"],
    )


def _laundering_linked_customers_have_documents(tables: dict[str, pl.DataFrame]) -> Check:
    documents = tables["documents"]
    missing = (
        tables["ground_truth/customer_labels"]
        .filter("laundering_linked")
        .join(documents, on="customer_id", how="anti")
    )
    # A world built without documents has none to miss.
    return _check(
        "every laundering-linked customer has documents, when documents are built",
        missing["customer_id"] if documents.height else pl.Series([], dtype=pl.String),
    )


def _articles_written_after_incorporation(tables: dict[str, pl.DataFrame]) -> Check:
    wrong = (
        tables["ground_truth/media_labels"]
        .filter(pl.col("subject_type") == "company")
        .join(tables["media/articles"], on="article_id")
        .join(tables["companies"], left_on="subject_id", right_on="customer_id")
        .filter(pl.col("published_on") < pl.col("incorporated_on"))
    )
    return _check("no article is about a company before it was incorporated", wrong["article_id"])


DOMAIN_CHECKS: list[Callable[[dict[str, pl.DataFrame]], Check]] = [
    _companies_owned_in_full,
    _customer_companies_have_a_director,
    _accounts_at_our_bank_have_a_customer,
    _laundering_linked_matches_transactions,
    _company_labels_agree_with_customer_labels,
    _attempt_transactions_are_laundering,
    _attempt_sizes_match_their_transactions,
    _amounts_are_positive,
    _customers_onboarded_before_their_activity,
    _officers_appointed_while_the_company_existed,
    _adults_when_they_sign,
    _registered_in_their_own_country,
    _customers_have_their_records,
    _shared_attributes_are_explained,
    _laundering_linked_customers_have_documents,
    _articles_written_after_incorporation,
]


def _document_files(tables: dict[str, pl.DataFrame], world: Path) -> Check:
    documents = tables["documents"]
    missing = [
        document_id
        for document_id, path in documents.select("document_id", "path").iter_rows()
        if not (world / path).is_file()
        or not (world / "ground_truth" / "documents" / f"{document_id}.json").is_file()
    ]
    return _check(
        "every document has its PDF and its ground truth on disk",
        pl.Series(missing, dtype=pl.String),
    )


def _documents_match_their_ground_truth(tables: dict[str, pl.DataFrame], world: Path) -> Check:
    wrong = []
    for label in tables["ground_truth/document_labels"].rows(named=True):
        path = world / "ground_truth" / "documents" / f"{label['document_id']}.json"
        if not path.is_file():
            continue  # reported by the files check
        truth = json.loads(path.read_text())
        injection = truth["injection"]
        if (
            [anomaly["type"] for anomaly in truth["anomalies"]] != label["anomalies"]
            or (injection["technique"] if injection else None) != label["injection"]
            or (injection["goal"] if injection else None) != label["injection_goal"]
            or truth["scanned"] != label["scanned"]
        ):
            wrong.append(label["document_id"])
    return _check(
        "document labels agree with each document's ground truth",
        pl.Series(wrong, dtype=pl.String),
    )


def _injections_are_in_the_pdf(tables: dict[str, pl.DataFrame], world: Path) -> Check:
    """A labelled injection an agent could never see would score a resistance it never showed."""
    injected = (
        tables["ground_truth/document_labels"]
        .filter(pl.col("injection").is_not_null())
        .join(tables["documents"].select("document_id", "path"), on="document_id")
    )
    missing = []
    for row in injected.rows(named=True):
        pdf_path = world / row["path"]
        truth = world / "ground_truth" / "documents" / f"{row['document_id']}.json"
        if not pdf_path.is_file() or not truth.is_file():
            continue  # reported by the files check
        payload = json.loads(truth.read_text())["injection"]["payload"]
        if row["scanned"]:
            # Only a visible remark survives a scan, and reading it back would take OCR.
            if row["injection"] != "visible_note":
                missing.append(row["document_id"])
        elif payload not in _pdf_text(pdf_path, metadata=row["injection"] == "metadata"):
            missing.append(row["document_id"])
    return _check(
        "every labelled injection is in its PDF, and only visible ones are scanned",
        pl.Series(missing, dtype=pl.String),
    )


def _pdf_text(path: Path, *, metadata: bool) -> str:
    document = pdfium.PdfDocument(path)
    try:
        if metadata:
            return str(document.get_metadata_dict().get("Subject", ""))
        pages = [page.get_textpage().get_text_bounded() for page in document]
        return " ".join(" ".join(pages).split())
    finally:
        document.close()


DOCUMENT_CHECKS: list[Callable[[dict[str, pl.DataFrame], Path], Check]] = [
    _document_files,
    _documents_match_their_ground_truth,
    _injections_are_in_the_pdf,
]


def _check(name: str, offending: pl.Series) -> Check:
    examples = offending.cast(pl.String).unique().sort().head(EXAMPLES).to_list()
    return Check(name=name, failures=offending.len(), examples=examples)


def _keys(frame: pl.DataFrame, columns: list[str]) -> pl.Series:
    return frame.select(pl.concat_str(columns, separator="/", ignore_nulls=True)).to_series()


def _signals(tables: dict[str, pl.DataFrame]) -> list[Signal]:
    labels = tables["ground_truth/customer_labels"]
    customers = set(labels["customer_id"])
    linked = set(labels.filter("laundering_linked")["customer_id"])
    with_documents = set(tables["documents"]["customer_id"])
    people = customers_of_people(
        tables["persons"], tables["companies"], tables["officers"], tables["shareholdings"]
    )

    def customers_of(subjects: pl.DataFrame) -> set[str]:
        companies = subjects.filter(pl.col("subject_type") == "company")["subject_id"]
        persons = subjects.filter(pl.col("subject_type") == "person").join(
            people, left_on="subject_id", right_on="person_id"
        )
        return set(companies) | set(persons["customer_id"])

    def anomaly(frame: pl.DataFrame, column: str, value: str) -> set[str]:
        return set(frame.filter(pl.col(column).list.contains(value))["customer_id"])

    company_labels = tables["ground_truth/company_labels"]
    screening = tables["ground_truth/screening_labels"]
    media = tables["ground_truth/media_labels"]
    document_labels = tables["ground_truth/document_labels"].join(
        tables["documents"].select("document_id", "customer_id"), on="document_id"
    )
    flagged_customers = {
        # Documents exist only for a labelled sample; see the README.
        "has a document pack": with_documents,
        "profile: declared volume far below activity": anomaly(
            labels, "anomalies", "declared_volume_far_below_activity"
        ),
        "profile: onboarded shortly before activity": anomaly(
            labels, "anomalies", "onboarded_shortly_before_activity"
        ),
        "profile: vague account purpose": anomaly(labels, "anomalies", "vague_account_purpose"),
        "KYC risk rating high": set(
            tables["kyc_profiles"].filter(pl.col("risk_rating") == "high")["customer_id"]
        ),
        "shares a device with another customer": _sharing(tables["customer_devices"], "device_id"),
        "shares an address with another customer": _sharing(
            tables["customer_addresses"], "address_id"
        ),
        "company: offshore holding": anomaly(company_labels, "flags", "offshore_holding"),
        "company: nominee director": anomaly(company_labels, "flags", "nominee_director"),
        "company: circular ownership": anomaly(company_labels, "flags", "circular_ownership"),
        "watchlist: true match": customers_of(screening.filter(pl.col("label") == "true_match")),
        "watchlist: namesake": customers_of(screening.filter(pl.col("label") == "false_positive")),
        "media: relevant adverse": customers_of(
            media.filter(pl.col("label") == "relevant_adverse")
        ),
        "media: namesake": customers_of(media.filter(pl.col("label") == "irrelevant_namesake")),
        "media: not adverse": customers_of(media.filter(pl.col("label") == "not_adverse")),
    }
    # Documents exist only for a sample that over-represents laundering, so they're measured
    # against that sample, not against every customer.
    flagged_documented = {
        "document: any anomaly": set(
            document_labels.filter(pl.col("anomalies").list.len() > 0)["customer_id"]
        ),
        "document: prompt injection": set(
            document_labels.filter(pl.col("injection").is_not_null())["customer_id"]
        ),
    }
    return [
        _signal(name, "customers", customers, linked, flagged)
        for name, flagged in flagged_customers.items()
    ] + [
        _signal(name, "customers with documents", with_documents, linked, flagged)
        for name, flagged in flagged_documented.items()
    ]


def _signal(
    name: str, population_name: str, population: set[str], linked: set[str], flagged: set[str]
) -> Signal:
    flagged = flagged & population
    linked = linked & population
    hits = len(flagged & linked)
    precision = hits / len(flagged) if flagged else None
    base_rate = len(linked) / len(population) if population else 0
    return Signal(
        name=name,
        population=population_name,
        population_size=len(population),
        laundering_linked=len(linked),
        flagged=len(flagged),
        flagged_laundering_linked=hits,
        expected_by_chance=round(len(flagged) * base_rate, 1),
        precision=_rounded(precision),
        recall=_rounded(hits / len(linked) if linked else None),
        lift=_rounded(precision / base_rate if precision is not None and base_rate else None),
    )


def _booking_days(tables: dict[str, pl.DataFrame]) -> list[BookingDay]:
    days = (
        tables["transactions"]
        .select("transaction_id", day=pl.col("booked_at").dt.date())
        .join(tables["ground_truth/transaction_labels"], on="transaction_id")
        .group_by("day")
        .agg(transactions=pl.len(), laundering=pl.col("is_laundering").sum())
        .sort("day")
    )
    return [
        BookingDay(day=day.isoformat(), transactions=transactions, laundering=laundering)
        for day, transactions, laundering in days.iter_rows()
    ]


def _sharing(links: pl.DataFrame, attribute: str) -> set[str]:
    shared = links.filter(pl.col("customer_id").n_unique().over(attribute) > 1)
    return set(shared["customer_id"])


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


def render_markdown(report: QualityReport) -> str:
    failed = report.failed_checks
    lines = [
        f"# Data-quality report: {report.world}",
        "",
        (
            f"**All {len(report.checks)} checks passed.**"
            if not failed
            else f"**{len(failed)} of {len(report.checks)} checks failed.**"
        ),
        "",
        "## Tables",
        "",
        "| Table | Rows | Columns with nulls |",
        "| --- | ---: | --- |",
    ]
    for name, table in report.tables.items():
        nulls = ", ".join(
            f"{column} {rate:.1%}" for column, rate in table.null_rates.items() if rate
        )
        lines.append(f"| {name} | {table.rows:,} | {nulls or '-'} |")
    lines += ["", "## Checks", "", "| Check | Result |", "| --- | --- |"]
    for check in report.checks:
        result = (
            "pass"
            if not check.failures
            else f"**fail**: {check.failures:,} rows, e.g. {', '.join(check.examples)}"
        )
        lines.append(f"| {check.name} | {result} |")
    lines += [
        "",
        "## Signals",
        "",
        "How much each planted red flag says about the laundering label. Precision is the share of "
        "flagged customers who are laundering-linked, recall the share of laundering-linked "
        "customers who are flagged, and lift is precision over the population's base rate: 1 "
        "means the signal says nothing. A useful signal has a lift well above 1 and a precision "
        "well below 100%, so it never gives the label away on its own. With few flagged "
        "customers, compare the laundering-linked hits with what chance alone would give.",
        "",
    ]
    for population in dict.fromkeys(signal.population for signal in report.signals):
        first = next(s for s in report.signals if s.population == population)
        base_rate = (
            first.laundering_linked / first.population_size if first.population_size else None
        )
        lines += [
            f"### Among {population}",
            "",
            f"{first.laundering_linked:,} of {first.population_size:,} are laundering-linked "
            f"({_percent(base_rate)}).",
            "",
            "| Signal | Flagged | Laundering-linked (by chance) | Precision | Recall | Lift |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for signal in report.signals:
            if signal.population == population:
                lift = "-" if signal.lift is None else f"{signal.lift:.1f}"
                lines.append(
                    f"| {signal.name} | {signal.flagged:,} | "
                    f"{signal.flagged_laundering_linked:,} ({signal.expected_by_chance:g}) | "
                    f"{_percent(signal.precision)} | {_percent(signal.recall)} | {lift} |"
                )
        lines.append("")
    lines += [
        "## Laundering by booking day",
        "",
        "AMLworld's laundering isn't spread evenly over time: a sparse tail of mostly laundering "
        "transactions follows the busy days, so the booking date alone flags some of it. "
        "Time-based splits and features have to allow for that.",
        "",
        "| Day | Transactions | Laundering | Share |",
        "| --- | ---: | ---: | ---: |",
    ]
    for day in report.booking_days:
        share = _percent(day.laundering / day.transactions)
        lines.append(f"| {day.day} | {day.transactions:,} | {day.laundering:,} | {share} |")
    lines.append("")
    return "\n".join(lines)


def _percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.1%}"

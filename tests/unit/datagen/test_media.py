from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import OUR_BANK, build_small_world, csv_row

from datagen.media import Media, assign_media
from datagen.media_corpus import IRRELEVANT_NAMESAKE, briefs

ACTIVITY_STARTS = date(2022, 9, 1)
WORLD_FILES = [
    "customers",
    "persons",
    "addresses",
    "customer_addresses",
    "companies",
    "officers",
    "ground_truth/customer_labels",
]


def corpus() -> list[dict[str, str]]:
    articles = []
    for brief in briefs():
        if brief.label == IRRELEVANT_NAMESAKE:
            body = "{name} ({other_age}) of {other_city}, {other_occupation}, was jailed."
        elif brief.subject_kind == "person":
            body = "{name} ({age}) of {city}, in {industry}: " + brief.story
        else:
            body = "{company} of {city}, in {industry}: " + brief.story
        articles.append(
            {
                "brief_id": brief.brief_id,
                "label": brief.label,
                "story": brief.story,
                "subject_kind": brief.subject_kind,
                "outlet": "The Invented Gazette",
                "headline": "News about {city}",
                "body": body,
            }
        )
    return articles


@pytest.fixture(scope="module")
def media(tmp_path_factory: pytest.TempPathFactory) -> Media:
    directory = tmp_path_factory.mktemp("world")
    rows = [csv_row(OUR_BANK, f"C{n}", "002", f"B{n}", laundering=n < 60) for n in range(400)]
    world = build_small_world(directory, rows)
    frames = {f"{name}.parquet": pl.read_parquet(world / f"{name}.parquet") for name in WORLD_FILES}
    return assign_media(frames, corpus(), ACTIVITY_STARTS, seed=4)


def test_fills_every_placeholder(media: Media) -> None:
    for headline, body in media.articles.select("headline", "body").rows():
        assert "{" not in headline + body


def test_relevant_coverage_clusters_around_laundering(media: Media) -> None:
    relevant = media.labels.filter(pl.col("label") == "relevant_adverse")

    assert relevant.height > 0
    assert media.labels.filter(pl.col("label") == "irrelevant_namesake").height > 0
    assert media.labels.filter(pl.col("label") == "not_adverse").height > 0


def test_articles_are_marked_fictional_and_dated_before_the_activity(media: Media) -> None:
    assert media.articles.get_column("disclaimer").str.contains("Fictional").all()
    assert media.articles.get_column("published_on").max() < ACTIVITY_STARTS  # type: ignore[operator]


def test_labels_point_at_existing_articles(media: Media) -> None:
    assert set(media.labels.get_column("article_id")) == set(
        media.articles.get_column("article_id")
    )


def test_an_empty_corpus_yields_no_articles(tmp_path: Path) -> None:
    world = build_small_world(tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1")])
    frames = {f"{name}.parquet": pl.read_parquet(world / f"{name}.parquet") for name in WORLD_FILES}

    assert assign_media(frames, [], ACTIVITY_STARTS, seed=4).articles.is_empty()

import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from datagen.media_corpus import (
    IRRELEVANT_NAMESAKE,
    ArticleDraft,
    Brief,
    ClaudeWriter,
    briefs,
    read_corpus,
    usable,
    write_corpus,
)

PERSON = next(b for b in briefs() if b.subject_kind == "person" and b.label != IRRELEVANT_NAMESAKE)
NAMESAKE = next(b for b in briefs() if b.label == IRRELEVANT_NAMESAKE)


def draft(body: str, headline: str = "Local news") -> ArticleDraft:
    return ArticleDraft(outlet="The Invented Gazette", headline=headline, body=body)


def good_draft(brief: Brief) -> ArticleDraft:
    if brief.label == IRRELEVANT_NAMESAKE:
        return draft("{name}, {other_age}, of {other_city} worked as {other_occupation}.")
    if brief.subject_kind == "person":
        return draft("{name}, {age}, of {city} works in {industry}.")
    return draft("{company} of {city} trades in {industry}.")


class FakeWriter:
    def __init__(self, failures: int = 0) -> None:
        self.calls: Counter[str] = Counter()
        self.failures = failures

    async def write(self, brief: Brief) -> ArticleDraft | None:
        self.calls[brief.brief_id] += 1
        if self.calls[brief.brief_id] <= self.failures:
            return draft("A story with no placeholders at all.")
        return good_draft(brief)


def test_briefs_cover_every_label_with_unique_ids() -> None:
    all_briefs = briefs()

    assert Counter(b.label for b in all_briefs) == {
        "relevant_adverse": 160,
        "irrelevant_namesake": 120,
        "not_adverse": 120,
    }
    assert len({b.brief_id for b in all_briefs}) == len(all_briefs)


@pytest.mark.parametrize(
    ("article", "brief", "ok"),
    [
        (draft("{name}, {age}, of {city}."), PERSON, True),
        (draft("{name} of somewhere."), PERSON, False),
        (draft("{name} of {city} and {company}."), PERSON, False),
        (draft("{name} of {city} {unbalanced"), PERSON, False),
        (draft("{name}, {other_age}, from {other_city}."), NAMESAKE, True),
        (draft("{name}, {age}, from {city}."), NAMESAKE, False),
    ],
)
def test_a_draft_must_use_its_placeholders_and_no_others(
    article: ArticleDraft, brief: Brief, ok: bool
) -> None:
    assert usable(article, brief) is ok


async def test_writes_every_brief_and_resumes_where_it_stopped(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus.jsonl"

    first = await write_corpus(FakeWriter(), corpus)
    again = await write_corpus(FakeWriter(), corpus)

    assert (first, again) == (len(briefs()), 0)
    articles = read_corpus(corpus)
    assert [a["brief_id"] for a in articles] == sorted(b.brief_id for b in briefs())


async def test_asks_again_when_a_draft_is_unusable(tmp_path: Path) -> None:
    writer = FakeWriter(failures=1)

    written = await write_corpus(writer, tmp_path / "corpus.jsonl")

    assert written == len(briefs())
    assert set(writer.calls.values()) == {2}


async def test_leaves_out_a_brief_that_keeps_failing(tmp_path: Path) -> None:
    written = await write_corpus(FakeWriter(failures=5), tmp_path / "corpus.jsonl", attempts=2)

    assert written == 0
    assert read_corpus(tmp_path / "corpus.jsonl") == []


class FakeMessages:
    def __init__(self, stop_reason: str, text: str) -> None:
        self.stop_reason, self.text = stop_reason, text
        self.request: dict[str, Any] = {}

    async def create(self, **request: Any) -> SimpleNamespace:
        self.request = request
        block = SimpleNamespace(type="text", text=self.text)
        return SimpleNamespace(stop_reason=self.stop_reason, content=[block])


def claude(messages: FakeMessages) -> ClaudeWriter:
    client = SimpleNamespace(beta=SimpleNamespace(messages=messages))
    return ClaudeWriter(cast(Any, client))


async def test_claude_writes_structured_json_with_its_placeholders() -> None:
    article = good_draft(PERSON)
    messages = FakeMessages("end_turn", article.model_dump_json())

    written = await claude(messages).write(PERSON)

    assert written == article
    request = messages.request
    assert request["output_config"]["format"]["type"] == "json_schema"
    assert "{name}" in request["system"]
    assert "{city}" in request["system"]
    assert request["messages"][0]["content"] == PERSON.prompt()


async def test_claude_gives_up_on_a_refusal() -> None:
    messages = FakeMessages("refusal", json.dumps({}))

    assert await claude(messages).write(PERSON) is None

"""The fictional adverse-media corpus: briefs, and the one-off LLM run that writes them up.

Articles are written once and committed, so world builds stay offline and deterministic.
They name their subject through placeholders that each world fills with its own people.
"""

import asyncio
import json
import re
import string
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import openai
from pydantic import BaseModel, ConfigDict

CORPUS = Path(__file__).parent / "corpus" / "adverse_media.jsonl"
MODEL = "gpt-5.5"
CONCURRENCY = 8

RELEVANT_ADVERSE = "relevant_adverse"
IRRELEVANT_NAMESAKE = "irrelevant_namesake"
NOT_ADVERSE = "not_adverse"

# What each kind of article is about, by label. Relevant stories are financial crime that
# fits the subject; namesake stories are crimes by someone else who shares the name, financial
# ones included, so that only the namesake's age, city and occupation tell the two apart.
STORIES = {
    RELEVANT_ADVERSE: [
        "has been charged over a VAT carousel fraud",
        "is under investigation for laundering money through shell companies",
        "has been accused of bribing a public official to win contracts",
        "has been fined for breaching EU sanctions on exports",
        "has been arrested as part of an invoice-fraud ring",
        "has been linked to an online investment scam",
        "has been accused of hiding income in offshore accounts",
        "has been named in a leaked report on money-mule networks",
    ],
    IRRELEVANT_NAMESAKE: [
        "has been convicted of armed robbery",
        "has been jailed for drug trafficking",
        "has been charged over a road-rage assault",
        "has been accused of embezzling from an amateur football club",
        "has been sentenced for a string of burglaries",
        "has been charged over a VAT carousel fraud",
        "has been convicted of laundering money for a drug gang",
        "has been arrested as part of an invoice-fraud ring",
        "has been accused of hiding income in offshore accounts",
    ],
    NOT_ADVERSE: [
        "has won a regional business award",
        "has opened a second location",
        "sponsors a youth sports club",
        "is expanding into export markets",
        "is featured in a profile of local entrepreneurs",
        "has raised money for a children's hospital",
    ],
}
# Per story and kind of subject: 160 relevant, 216 namesake and 120 harmless articles.
ARTICLES_PER_STORY = {RELEVANT_ADVERSE: 10, IRRELEVANT_NAMESAKE: 24, NOT_ADVERSE: 10}

# The placeholders an article may use, and those it must, by kind of subject.
PERSON_PLACEHOLDERS = {"name", "city", "age", "industry"}
COMPANY_PLACEHOLDERS = {"company", "city", "industry"}
NAMESAKE_PLACEHOLDERS = {"name", "other_city", "other_age", "other_occupation"}
REQUIRED = {
    "person": {"name", "city"},
    "company": {"company", "city"},
    "namesake": {"name", "other_city", "other_age"},
}

SYSTEM = """You write short fictional local-news articles for a synthetic dataset used to test \
anti-money-laundering software. Everything you write is invented: never name a real person, \
company or news outlet. Invent the outlet's name.

Refer to the subject only through these placeholders, written exactly with their braces, and \
use no other braces: {placeholders}. Use each of them at least once. Write 120 to 220 words in \
a neutral news style, with a headline."""


@dataclass(frozen=True)
class Brief:
    brief_id: str
    label: str
    story: str
    subject_kind: str
    placeholders: tuple[str, ...]

    def prompt(self) -> str:
        if self.label == IRRELEVANT_NAMESAKE:
            return (
                f"Write an article about a person who {self.story}. They live in {{other_city}}, "
                "are {other_age} years old and work as {other_occupation}."
            )
        if self.subject_kind == "person":
            return (
                "Write an article about a person from {city}, aged {age}, working in "
                f"{{industry}}, who {self.story}."
            )
        return (
            "Write an article about a small company in {industry} from {city} "
            f"that {self.story}."
        )


class CorpusWriterError(Exception):
    """The model can't be reached at all (no credits, a bad key), so retrying won't help."""


class ArticleDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outlet: str
    headline: str
    body: str


class Writer(Protocol):
    async def write(self, brief: Brief) -> ArticleDraft | None: ...


def briefs() -> list[Brief]:
    result = []
    for label, stories in STORIES.items():
        kinds = ["person"] if label == IRRELEVANT_NAMESAKE else ["person", "company"]
        for story_number, story in enumerate(stories):
            for kind in kinds:
                placeholders = (
                    NAMESAKE_PLACEHOLDERS
                    if label == IRRELEVANT_NAMESAKE
                    else PERSON_PLACEHOLDERS
                    if kind == "person"
                    else COMPANY_PLACEHOLDERS
                )
                for copy in range(ARTICLES_PER_STORY[label]):
                    result.append(
                        Brief(
                            brief_id=f"{label}-{story_number:02d}-{kind}-{copy:02d}",
                            label=label,
                            story=story,
                            subject_kind=kind,
                            placeholders=tuple(sorted(placeholders)),
                        )
                    )
    return result


def placeholders_in(text: str) -> set[str]:
    return {field for _, field, _, _ in string.Formatter().parse(text) if field}


def usable(draft: ArticleDraft, brief: Brief) -> bool:
    """Every required placeholder appears, and nothing else in braces does."""
    try:
        found = placeholders_in(draft.headline + "\n" + draft.body)
    except ValueError:  # an unbalanced brace
        return False
    kind = "namesake" if brief.label == IRRELEVANT_NAMESAKE else brief.subject_kind
    required = REQUIRED[kind]
    return required <= found <= set(brief.placeholders) and not re.search(r"\{\{|\}\}", draft.body)


class OpenAIWriter:
    def __init__(self, client: openai.AsyncOpenAI | None = None) -> None:
        self._client = client or openai.AsyncOpenAI()

    async def write(self, brief: Brief) -> ArticleDraft | None:
        placeholders = ", ".join(f"{{{name}}}" for name in brief.placeholders)
        try:
            response = await self._client.responses.parse(
                model=MODEL,
                # Short news pieces don't need deep reasoning.
                reasoning={"effort": "low"},
                max_output_tokens=4000,
                instructions=SYSTEM.replace("{placeholders}", placeholders),
                input=brief.prompt(),
                text_format=ArticleDraft,
            )
        except (openai.AuthenticationError, openai.PermissionDeniedError) as error:
            raise CorpusWriterError(f"{type(error).__name__}: {error}") from error
        except openai.RateLimitError as error:
            if error.code == "insufficient_quota":
                raise CorpusWriterError(f"{type(error).__name__}: {error}") from error
            return None  # a burst past the SDK's own retries; the brief is tried again
        # A refusal or a cut-off answer leaves nothing parsed.
        if response.status != "completed":
            return None
        return response.output_parsed


async def write_corpus(writer: Writer, corpus: Path = CORPUS, attempts: int = 2) -> int:
    """Write the briefs not yet in the corpus, appending as they finish so a run can resume."""
    done = {article["brief_id"] for article in read_corpus(corpus)}
    pending = [brief for brief in briefs() if brief.brief_id not in done]
    corpus.parent.mkdir(parents=True, exist_ok=True)
    gate = asyncio.Semaphore(CONCURRENCY)
    written = 0

    async def write_one(brief: Brief) -> None:
        nonlocal written
        async with gate:
            for _ in range(attempts):
                draft = await writer.write(brief)
                if draft is not None and usable(draft, brief):
                    record = {**asdict(brief), **draft.model_dump()}
                    with corpus.open("a", encoding="utf-8") as file:
                        file.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written += 1
                    return

    await asyncio.gather(*(write_one(brief) for brief in pending))
    return written


def read_corpus(corpus: Path = CORPUS) -> list[dict[str, Any]]:
    if not corpus.is_file():
        return []
    articles = [json.loads(line) for line in corpus.read_text(encoding="utf-8").splitlines()]
    # Appends finish in any order; sorting keeps world builds independent of that.
    return sorted(articles, key=lambda article: str(article["brief_id"]))

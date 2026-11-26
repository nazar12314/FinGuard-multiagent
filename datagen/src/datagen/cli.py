import argparse
import asyncio
import json
import os
import sys
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

from datagen.amlworld import AmlworldFormatError, canonical_bank_id
from datagen.media_corpus import CORPUS, OpenAIWriter, briefs, read_corpus, write_corpus
from datagen.replay import ONE_DAY_PER_MINUTE, Booking, Pace, bookings, replay
from datagen.world import WorldConfig, build_world, world_dir
from finguard.core.settings import KafkaSettings
from finguard.infrastructure.kafka.publisher import KafkaPublisher, create_kafka_producer

PROGRESS_EVERY = 5000


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="finguard-datagen")
    commands = parser.add_subparsers(dest="command", required=True)
    world = argparse.ArgumentParser(add_help=False)
    world.add_argument("--seed", type=int, required=True)
    world.add_argument("--worlds-dir", type=Path, default=Path("data/worlds"))

    build = commands.add_parser("build", parents=[world], help="build a seeded world")
    build.add_argument("--variant", default="HI-Small", help="AMLworld variant, e.g. HI-Small")
    build.add_argument("--our-bank", default="12", help="AMLworld bank id that plays FinGuard")
    build.add_argument(
        "--document-sample",
        type=int,
        default=WorldConfig.document_sample,
        help="benign customers who get documents, besides every laundering-linked one",
    )
    build.add_argument(
        "--no-documents",
        dest="documents",
        action="store_false",
        help="skip rendering PDFs, for a quick build",
    )
    build.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="holds amlworld/ (Kaggle files) and opensanctions/ (targets.simple.csv files)",
    )

    replay_command = commands.add_parser(
        "replay", parents=[world], help="stream a built world's transactions into tx.raw"
    )
    replay_command.add_argument(
        "--speedup",
        type=float,
        default=ONE_DAY_PER_MINUTE,
        help="simulated seconds per real second (default: one day per minute; inf: no waiting)",
    )
    replay_command.add_argument("--max-rate", type=float, help="cap on messages per second")
    replay_command.add_argument("--limit", type=int, help="replay only the first N transactions")
    replay_command.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "FINGUARD_KAFKA__BOOTSTRAP_SERVERS", KafkaSettings().bootstrap_servers
        ),
    )
    commands.add_parser(
        "media",
        help="write the fictional adverse-media corpus with an LLM (once; the result is committed)",
    )
    arguments = parser.parse_args(argv)

    try:
        match arguments.command:
            case "build":
                _build(arguments)
            case "replay":
                _replay(arguments)
            case "media":
                _media()
    except (FileNotFoundError, AmlworldFormatError) as error:
        sys.exit(f"finguard-datagen: {error}")


def _build(arguments: argparse.Namespace) -> None:
    config = WorldConfig(
        seed=arguments.seed,
        amlworld_variant=arguments.variant,
        our_bank=canonical_bank_id(arguments.our_bank),
        document_sample=arguments.document_sample,
        documents=arguments.documents,
    )
    world = build_world(config, arguments.raw_dir, arguments.worlds_dir)
    counts = json.loads((world / "manifest.json").read_text())["counts"]
    print(f"built {world}: " + ", ".join(f"{name} {value}" for name, value in counts.items()))
    if not read_corpus():
        print(f"note: no adverse-media corpus at {CORPUS}; run `finguard-datagen media` once")


def _media() -> None:
    written = asyncio.run(write_corpus(OpenAIWriter()))
    total, done = len(briefs()), len(read_corpus())
    print(f"wrote {written} articles; the corpus has {done} of {total} at {CORPUS}")
    if done < total:
        sys.exit("finguard-datagen: some briefs failed; run `media` again to retry them")


def _replay(arguments: argparse.Namespace) -> None:
    if arguments.speedup <= 0 or (arguments.max_rate is not None and arguments.max_rate <= 0):
        sys.exit("finguard-datagen: --speedup and --max-rate must be positive")
    world = world_dir(arguments.worlds_dir, arguments.seed)
    pace = Pace(speedup=arguments.speedup, max_messages_per_s=arguments.max_rate)
    messages = _with_progress(bookings(world, arguments.limit))
    sent = asyncio.run(_publish(messages, arguments.bootstrap_servers, pace))
    print(f"replayed {sent} transactions from {world} to tx.raw")


async def _publish(messages: Iterable[Booking], bootstrap_servers: str, pace: Pace) -> int:
    producer = create_kafka_producer(bootstrap_servers)
    await producer.start()
    try:
        return await replay(messages, KafkaPublisher(producer), pace)
    finally:
        await producer.stop()


def _with_progress(messages: Iterable[Booking]) -> Iterator[Booking]:
    for count, booking in enumerate(messages, start=1):
        yield booking
        if count % PROGRESS_EVERY == 0:
            print(f"replayed {count} (booked {booking.booked_at:%Y-%m-%d %H:%M})", flush=True)

import asyncio
import signal
import time
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from typing import Protocol

from finguard.core.container import build_outbox_relay
from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.settings import OutboxRelaySettings, Settings, load_settings_or_exit

logger = get_logger(__name__)


class Relay(Protocol):
    async def publish_pending(self) -> int: ...

    async def prune_published(self, older_than: timedelta) -> int: ...


async def relay_until_stopped(
    relay: Relay, settings: OutboxRelaySettings, stop: asyncio.Event
) -> None:
    retention = timedelta(days=settings.retention_days)
    next_prune_at = time.monotonic()

    while not stop.is_set():
        if time.monotonic() >= next_prune_at:
            next_prune_at = time.monotonic() + settings.prune_interval_s
            try:
                pruned = await relay.prune_published(retention)
            except Exception:
                logger.exception(LogEvent.OUTBOX_PRUNE_FAILED)
            else:
                logger.info(LogEvent.OUTBOX_PRUNED, count=pruned)

        try:
            published = await relay.publish_pending()
        except Exception:
            # Kafka or Postgres is unavailable: the messages stay pending and are retried.
            logger.exception(LogEvent.OUTBOX_PUBLISH_FAILED)
            published = 0
        if published:
            logger.info(LogEvent.OUTBOX_BATCH_PUBLISHED, count=published)

        # A full batch means more messages are probably waiting, so go again at once.
        if published < settings.batch_size:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.poll_interval_s)


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stop.set)

    logger.info(LogEvent.APP_STARTING, env=settings.env, process="outbox-relay")
    async with AsyncExitStack() as resources:
        relay = await build_outbox_relay(settings, resources)
        await relay_until_stopped(relay, settings.outbox_relay, stop)
    logger.info(LogEvent.APP_STOPPED)


def main() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()

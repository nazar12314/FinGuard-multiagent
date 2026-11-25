import asyncio
import signal
import time
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from typing import Protocol

from finguard.core.container import build_outbox_relay
from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.settings import OutboxRelaySettings, Settings, load_settings_or_exit
from finguard.core.telemetry import configure_telemetry
from finguard.infrastructure.outbox.relay import MessageParked

logger = get_logger(__name__)


class Relay(Protocol):
    async def publish_pending(self) -> int: ...

    async def prune_published(self, older_than: timedelta) -> int: ...


class RelayMustRestart(Exception):
    """The relay can't make progress with its current connections."""


def retry_delay(consecutive_failures: int, settings: OutboxRelaySettings) -> float:
    exponential: float = settings.poll_interval_s * 2 ** (consecutive_failures - 1)
    return min(exponential, settings.max_retry_delay_s)


async def relay_until_stopped(
    relay: Relay, settings: OutboxRelaySettings, stop: asyncio.Event
) -> None:
    """Publish until `stop` is set. Raise RelayMustRestart when a fresh process is needed."""
    retention = timedelta(days=settings.retention_days)
    next_prune_at = time.monotonic()
    consecutive_failures = 0

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
        except MessageParked as parked:
            logger.error(
                LogEvent.OUTBOX_MESSAGE_PARKED,
                message_id=str(parked.message_id),
                topic=parked.topic,
                reason=parked.reason,
            )
            # The producer may be unusable for that partition after a refusal.
            raise RelayMustRestart from parked
        except TimeoutError as timeout:
            logger.error(LogEvent.OUTBOX_PUBLISH_TIMED_OUT, timeout_s=settings.publish_timeout_s)
            raise RelayMustRestart from timeout
        except Exception:
            # Kafka or Postgres is unavailable: the messages stay pending and are retried.
            consecutive_failures += 1
            logger.exception(LogEvent.OUTBOX_PUBLISH_FAILED, attempt=consecutive_failures)
            if consecutive_failures >= settings.max_consecutive_failures:
                logger.error(LogEvent.OUTBOX_RELAY_GAVE_UP, attempts=consecutive_failures)
                raise RelayMustRestart from None
            await _wait(stop, retry_delay(consecutive_failures, settings))
            continue

        consecutive_failures = 0
        if published:
            logger.info(LogEvent.OUTBOX_BATCH_PUBLISHED, count=published)
        # A full batch means more messages are probably waiting, so go again at once.
        if published < settings.batch_size:
            await _wait(stop, settings.poll_interval_s)


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


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
    flush_telemetry = configure_telemetry(settings, process="outbox-relay")
    try:
        asyncio.run(run(settings))
    except RelayMustRestart:
        # Pending messages stay in the outbox; the supervisor restarts the process.
        raise SystemExit(1) from None
    finally:
        flush_telemetry()


if __name__ == "__main__":
    main()

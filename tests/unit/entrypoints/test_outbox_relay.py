import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from finguard.core.settings import OutboxRelaySettings
from finguard.entrypoints.outbox_relay import RelayMustRestart, relay_until_stopped, retry_delay
from finguard.infrastructure.outbox.relay import MessageParked

LogLines = Callable[[], list[dict[str, Any]]]


class ScriptedRelay:
    """Returns the scripted batch sizes in turn, then asks the loop to stop."""

    def __init__(self, batches: list[int | Exception], stop: asyncio.Event) -> None:
        self._batches = batches
        self._stop = stop
        self.publish_calls = 0
        self.prune_retentions: list[timedelta] = []
        self.prune_error: Exception | None = None
        self.backlog_measurements = 0
        self.backlog_error: Exception | None = None

    async def publish_pending(self) -> int:
        self.publish_calls += 1
        if not self._batches:
            self._stop.set()
            return 0
        batch = self._batches.pop(0)
        if isinstance(batch, Exception):
            raise batch
        return batch

    async def measure_backlog(self) -> None:
        self.backlog_measurements += 1
        if self.backlog_error:
            raise self.backlog_error

    async def prune_published(self, older_than: timedelta) -> int:
        self.prune_retentions.append(older_than)
        if self.prune_error:
            raise self.prune_error
        return 3


async def run(relay: ScriptedRelay, stop: asyncio.Event, **settings: Any) -> None:
    await asyncio.wait_for(
        relay_until_stopped(relay, OutboxRelaySettings(**settings), stop), timeout=2
    )


def events(log_lines: LogLines) -> list[str]:
    return [line["event"] for line in log_lines()]


async def test_full_batches_are_published_back_to_back() -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([2, 2], stop)

    # A one-minute poll interval would time the test out if the loop waited between batches.
    await run(relay, stop, batch_size=2, poll_interval_s=60)

    assert relay.publish_calls == 3


async def test_stopping_interrupts_the_wait_between_polls() -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([1, 1], stop)
    asyncio.get_running_loop().call_later(0.05, stop.set)

    await run(relay, stop, poll_interval_s=60)

    assert relay.publish_calls == 1


async def test_a_failed_publish_is_logged_and_retried(log_lines: LogLines) -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([ConnectionError("kafka unavailable"), 1], stop)

    await run(relay, stop, poll_interval_s=0.01)

    assert "outbox.publish_failed" in events(log_lines)
    assert "outbox.batch_published" in events(log_lines)


async def test_the_relay_gives_up_after_repeated_failures(log_lines: LogLines) -> None:
    stop = asyncio.Event()
    failures: list[int | Exception] = [ConnectionError("kafka unavailable")] * 3
    relay = ScriptedRelay(failures, stop)

    with pytest.raises(RelayMustRestart):
        await run(relay, stop, poll_interval_s=0.01, max_consecutive_failures=3)

    assert relay.publish_calls == 3
    assert "outbox.relay_gave_up" in events(log_lines)


@pytest.mark.parametrize(
    ("error", "event"),
    [
        (MessageParked(uuid4(), "cases.events", "too large"), "outbox.message_parked"),
        (TimeoutError(), "outbox.publish_timed_out"),
    ],
)
async def test_a_parked_message_or_a_stuck_producer_restarts_the_relay(
    log_lines: LogLines, error: Exception, event: str
) -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([error, 1], stop)

    with pytest.raises(RelayMustRestart):
        await run(relay, stop)

    assert relay.publish_calls == 1
    assert event in events(log_lines)


def test_retries_back_off_exponentially_up_to_a_cap() -> None:
    settings = OutboxRelaySettings(poll_interval_s=1, max_retry_delay_s=10)

    assert [retry_delay(failures, settings) for failures in range(1, 7)] == [1, 2, 4, 8, 10, 10]


async def test_old_published_messages_are_pruned_at_startup() -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([], stop)

    await run(relay, stop, retention_days=7)

    assert relay.prune_retentions == [timedelta(days=7)]


async def test_a_failed_prune_does_not_stop_publishing(log_lines: LogLines) -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([1], stop)
    relay.prune_error = ConnectionError("database unavailable")

    await run(relay, stop, poll_interval_s=0.01)

    assert relay.publish_calls == 2
    assert "outbox.prune_failed" in events(log_lines)


async def test_the_loop_returns_once_stopped() -> None:
    stop = asyncio.Event()
    stop.set()
    relay = ScriptedRelay([1], stop)

    await run(relay, stop)

    assert relay.publish_calls == 0


async def test_the_backlog_is_measured_after_every_publish(log_lines: LogLines) -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([1, 1], stop)
    relay.backlog_error = ConnectionError("database unavailable")

    await run(relay, stop, poll_interval_s=0.01)

    # A failed measurement is logged and doesn't stop publishing.
    assert relay.backlog_measurements == 3
    assert relay.publish_calls == 3
    assert "outbox.backlog_unmeasured" in events(log_lines)

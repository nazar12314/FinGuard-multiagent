import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from finguard.core.settings import OutboxRelaySettings
from finguard.entrypoints.outbox_relay import relay_until_stopped

LogLines = Callable[[], list[dict[str, Any]]]


class ScriptedRelay:
    """Returns the scripted batch sizes in turn, then asks the loop to stop."""

    def __init__(self, batches: list[int | Exception], stop: asyncio.Event) -> None:
        self._batches = batches
        self._stop = stop
        self.publish_calls = 0
        self.prune_retentions: list[timedelta] = []
        self.prune_error: Exception | None = None

    async def publish_pending(self) -> int:
        self.publish_calls += 1
        if not self._batches:
            self._stop.set()
            return 0
        batch = self._batches.pop(0)
        if isinstance(batch, Exception):
            raise batch
        return batch

    async def prune_published(self, older_than: timedelta) -> int:
        self.prune_retentions.append(older_than)
        if self.prune_error:
            raise self.prune_error
        return 3


async def run(relay: ScriptedRelay, stop: asyncio.Event, **settings: Any) -> None:
    await asyncio.wait_for(
        relay_until_stopped(relay, OutboxRelaySettings(**settings), stop), timeout=2
    )


async def test_full_batches_are_published_back_to_back() -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([2, 2], stop)

    # A one-minute poll interval would time the test out if the loop waited between batches.
    await run(relay, stop, batch_size=2, poll_interval_s=60)

    assert relay.publish_calls == 3


async def test_a_failed_publish_is_logged_and_retried(log_lines: LogLines) -> None:
    stop = asyncio.Event()
    relay = ScriptedRelay([ConnectionError("kafka unavailable"), 1], stop)

    await run(relay, stop, poll_interval_s=0.01)

    events = [line["event"] for line in log_lines()]
    assert "outbox.publish_failed" in events
    assert "outbox.batch_published" in events


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
    assert "outbox.prune_failed" in [line["event"] for line in log_lines()]


async def test_the_loop_returns_once_stopped() -> None:
    stop = asyncio.Event()
    stop.set()
    relay = ScriptedRelay([1], stop)

    await run(relay, stop)

    assert relay.publish_calls == 0

import asyncio
import signal
from contextlib import AsyncExitStack

from finguard.core.container import build_message_consumer, open_session_factory
from finguard.core.diagnostics import PING_CONSUMER, PING_REQUESTED, ping_handler
from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.settings import Settings, load_settings_or_exit
from finguard.platform.topics import DIAGNOSTICS_PINGS

logger = get_logger(__name__)


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, stop.set)

    logger.info(LogEvent.APP_STARTING, env=settings.env, process="ping-consumer")
    async with AsyncExitStack() as resources:
        consumer = await build_message_consumer(
            settings,
            resources,
            name=PING_CONSUMER,
            topics=[DIAGNOSTICS_PINGS],
            handlers={PING_REQUESTED: ping_handler(open_session_factory(settings, resources))},
        )
        await consumer.run(stop)
    logger.info(LogEvent.APP_STOPPED)


def main() -> None:
    """Handle the dev-only pings sent through POST /api/v1/dev/pings."""
    settings = load_settings_or_exit()
    configure_logging(settings)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()

import asyncio
from contextlib import AsyncExitStack

from finguard.core.container import (
    build_message_consumer,
    consumer_heartbeat,
    open_session_factory,
)
from finguard.core.diagnostics import PING_CONSUMER, PING_REQUESTED, ping_handler
from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.ops import Readiness, start_ops_server, stop_on_signals
from finguard.core.settings import Settings, load_settings_or_exit
from finguard.core.telemetry import configure_telemetry
from finguard.platform.topics import DIAGNOSTICS_PINGS

logger = get_logger(__name__)

OPS_PORT = 9102


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    readiness = Readiness()
    heartbeat = consumer_heartbeat(settings)
    stop_on_signals(stop, readiness)

    logger.info(LogEvent.APP_STARTING, env=settings.env, process="ping-consumer")
    async with AsyncExitStack() as resources:
        consumer = await build_message_consumer(
            settings,
            resources,
            name=PING_CONSUMER,
            topics=[DIAGNOSTICS_PINGS],
            handlers={PING_REQUESTED: ping_handler(open_session_factory(settings, resources))},
            readiness=readiness,
            heartbeat=heartbeat,
        )
        await start_ops_server(settings.ops, readiness, heartbeat, resources, default_port=OPS_PORT)
        await consumer.run(stop)
    logger.info(LogEvent.APP_STOPPED)


def main() -> None:
    """Handle the dev-only pings sent through POST /api/v1/dev/pings."""
    settings = load_settings_or_exit()
    configure_logging(settings)
    flush_telemetry = configure_telemetry(settings, process="ping-consumer")
    try:
        asyncio.run(run(settings))
    finally:
        flush_telemetry()


if __name__ == "__main__":
    main()

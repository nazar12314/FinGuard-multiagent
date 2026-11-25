import signal
from types import FrameType

import uvicorn

from finguard.core.http.app import create_app
from finguard.core.logging import configure_logging
from finguard.core.settings import load_settings_or_exit
from finguard.core.telemetry import configure_telemetry


def _exit_on_signal(signal_number: int, frame: FrameType | None) -> None:
    raise SystemExit(128 + signal_number)


def main() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings)
    flush_telemetry = configure_telemetry(settings, process="api")
    # After a graceful shutdown uvicorn raises the signal again under the handler it found.
    # The default handler would kill the process before the spans below are flushed.
    signal.signal(signal.SIGTERM, _exit_on_signal)
    try:
        uvicorn.run(
            create_app(settings),
            host=settings.api.host,
            port=settings.api.port,
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=settings.api.graceful_shutdown_timeout_s,
        )
    finally:
        flush_telemetry()


if __name__ == "__main__":
    main()

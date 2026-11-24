import uvicorn

from finguard.core.http.app import create_app
from finguard.core.logging import configure_logging
from finguard.core.settings import load_settings_or_exit


def main() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings)
    uvicorn.run(
        create_app(settings),
        host=settings.api.host,
        port=settings.api.port,
        log_config=None,
        access_log=False,
        timeout_graceful_shutdown=settings.api.graceful_shutdown_timeout_s,
    )


if __name__ == "__main__":
    main()

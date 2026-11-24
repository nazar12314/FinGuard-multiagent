import sys

import uvicorn
from pydantic import BaseModel, ValidationError

from finguard.core.http.app import create_app
from finguard.core.logging import configure_logging
from finguard.core.settings import ENV_NESTED_DELIMITER, ENV_PREFIX, Settings, get_settings


def _env_var_hint(location: tuple[int | str, ...]) -> str:
    name = ENV_PREFIX + ENV_NESTED_DELIMITER.join(str(part).upper() for part in location)
    field = Settings.model_fields.get(str(location[0])) if len(location) == 1 else None
    is_section = (
        field is not None
        and isinstance(field.annotation, type)
        and issubclass(field.annotation, BaseModel)
    )
    return f"{name}{ENV_NESTED_DELIMITER}*" if is_section else name


def load_settings_or_exit() -> Settings:
    try:
        return get_settings()
    except ValidationError as exc:
        for error in exc.errors(include_input=False):
            path = ".".join(str(part) for part in error["loc"]) or "settings"
            hint = f" ({_env_var_hint(error['loc'])})" if error["loc"] else ""
            print(f"config error: {path}{hint}: {error['msg']}", file=sys.stderr)
        raise SystemExit(2) from None


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

import sys
from functools import lru_cache
from typing import Literal, Self

from pydantic import BaseModel, Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = "FINGUARD_"
ENV_NESTED_DELIMITER = "__"

Environment = Literal["dev", "test", "prod"]


class ApiSettings(BaseModel):
    host: str = "0.0.0.0"  # inside a container the API must listen on all interfaces
    port: int = Field(default=8000, ge=1, le=65535)
    graceful_shutdown_timeout_s: int = Field(default=20, ge=0)


class AuthSettings(BaseModel):
    # JWT joins this list when real authentication lands.
    mode: Literal["dev_header"] = "dev_header"


class DatabaseSettings(BaseModel):
    # The default matches the local docker-compose stack.
    url: SecretStr = SecretStr("postgresql+asyncpg://finguard:finguard@localhost:5432/finguard")
    pool_size: int = Field(default=5, ge=1)


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"


class OutboxRelaySettings(BaseModel):
    poll_interval_s: float = Field(default=1.0, gt=0)
    batch_size: int = Field(default=100, ge=1)
    retention_days: int = Field(default=7, ge=1)
    prune_interval_s: float = Field(default=3600.0, gt=0)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter=ENV_NESTED_DELIMITER,
        env_ignore_empty=True,
        extra="ignore",
    )

    # No default: a deployment that forgets FINGUARD_ENV must not boot as dev.
    env: Environment
    service_name: str = "finguard"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    api: ApiSettings = ApiSettings()
    auth: AuthSettings = AuthSettings()
    database: DatabaseSettings = DatabaseSettings()
    kafka: KafkaSettings = KafkaSettings()
    outbox_relay: OutboxRelaySettings = OutboxRelaySettings()

    @model_validator(mode="after")
    def _no_dev_auth_in_prod(self) -> Self:
        if self.env == "prod" and self.auth.mode == "dev_header":
            raise ValueError("auth.mode 'dev_header' trusts client headers and is not for prod")
        return self

    @property
    def docs_enabled(self) -> bool:
        return self.env != "prod"


@lru_cache
def get_settings() -> Settings:
    return Settings()


def load_settings_or_exit() -> Settings:
    """Load settings, or print each invalid value with the env var to fix and exit."""
    try:
        return get_settings()
    except ValidationError as exc:
        for error in exc.errors(include_input=False):
            path = ".".join(str(part) for part in error["loc"]) or "settings"
            hint = f" ({_env_var_hint(error['loc'])})" if error["loc"] else ""
            print(f"config error: {path}{hint}: {error['msg']}", file=sys.stderr)
        raise SystemExit(2) from None


def _env_var_hint(location: tuple[int | str, ...]) -> str:
    name = ENV_PREFIX + ENV_NESTED_DELIMITER.join(str(part).upper() for part in location)
    field = Settings.model_fields.get(str(location[0])) if len(location) == 1 else None
    is_section = (
        field is not None
        and isinstance(field.annotation, type)
        and issubclass(field.annotation, BaseModel)
    )
    return f"{name}{ENV_NESTED_DELIMITER}*" if is_section else name

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
    # The default matches the local docker-compose stack; prod must set it explicitly.
    url: SecretStr = SecretStr("postgresql+asyncpg://finguard:finguard@localhost:5432/finguard")
    pool_size: int = Field(default=5, ge=1)
    # Postgres ends a session that sits idle inside a transaction this long, so a stuck
    # process can't hold locks or block vacuum indefinitely.
    idle_in_transaction_timeout_s: float = Field(default=60.0, gt=0)


class KafkaSettings(BaseModel):
    # The default matches the local docker-compose stack; prod must set it explicitly.
    bootstrap_servers: str = "localhost:9092"
    replication_factor: int = Field(default=1, ge=1)
    # One retry topic per delay. Adding a delay needs `finguard-topics` to run again.
    # Before removing one, let its topic drain: consumers only read the configured tiers.
    retry_delays_s: tuple[float, ...] = (30.0, 300.0)


class ConsumerSettings(BaseModel):
    # In-process attempts before a failing message moves to the next retry topic.
    attempts: int = Field(default=3, ge=1)
    attempt_backoff_s: float = Field(default=0.5, ge=0)
    handler_timeout_s: float = Field(default=60.0, gt=0)


class TelemetrySettings(BaseModel):
    # Off unless asked for, so a stack without the collector doesn't log export failures.
    enabled: bool = False
    # The default matches the local docker-compose stack; prod must set it when enabled.
    otlp_endpoint: str = "http://localhost:4318"


class OutboxRelaySettings(BaseModel):
    poll_interval_s: float = Field(default=1.0, gt=0)
    batch_size: int = Field(default=100, ge=1)
    # Upper bound on publishing one batch. Past it the producer is assumed stuck and the
    # process exits, so it must stay below the database's idle-in-transaction timeout.
    publish_timeout_s: float = Field(default=10.0, gt=0)
    max_retry_delay_s: float = Field(default=30.0, gt=0)
    # After this many failed attempts in a row the process exits and starts afresh.
    max_consecutive_failures: int = Field(default=5, ge=1)
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
    consumer: ConsumerSettings = ConsumerSettings()
    telemetry: TelemetrySettings = TelemetrySettings()
    outbox_relay: OutboxRelaySettings = OutboxRelaySettings()

    @model_validator(mode="after")
    def _safe_for_prod(self) -> Self:
        if self.env != "prod":
            return self
        problems = []
        if self.auth.mode == "dev_header":
            problems.append("auth.mode 'dev_header' trusts client headers and is not for prod")
        # A forgotten variable must fail at startup, not fall back to the local stack.
        if "url" not in self.database.model_fields_set:
            problems.append("database.url must be set explicitly in prod")
        if "bootstrap_servers" not in self.kafka.model_fields_set:
            problems.append("kafka.bootstrap_servers must be set explicitly in prod")
        if "replication_factor" not in self.kafka.model_fields_set:
            problems.append("kafka.replication_factor must be set explicitly in prod")
        if "enabled" not in self.telemetry.model_fields_set:
            problems.append("telemetry.enabled must be set explicitly in prod")
        if self.telemetry.enabled and "otlp_endpoint" not in self.telemetry.model_fields_set:
            problems.append("telemetry.otlp_endpoint must be set explicitly in prod")
        if problems:
            raise ValueError("; ".join(problems))
        return self

    @property
    def docs_enabled(self) -> bool:
        return self.env != "prod"

    @property
    def dev_endpoints_enabled(self) -> bool:
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

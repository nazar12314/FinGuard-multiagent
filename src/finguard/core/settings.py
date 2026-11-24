from functools import lru_cache
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter=ENV_NESTED_DELIMITER,
        env_ignore_empty=True,
        extra="ignore",
    )

    env: Environment = "dev"
    service_name: str = "finguard"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    api: ApiSettings = ApiSettings()
    auth: AuthSettings = AuthSettings()

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

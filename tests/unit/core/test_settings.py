import pytest
from pydantic import ValidationError

from finguard.core.settings import Environment, Settings


def test_nested_settings_are_read_from_prefixed_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINGUARD_ENV", "test")
    monkeypatch.setenv("FINGUARD_API__PORT", "9100")

    settings = Settings()

    assert settings.env == "test"
    assert settings.api.port == 9100


def test_the_environment_must_be_set_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FINGUARD_ENV", raising=False)

    with pytest.raises(ValidationError, match="env"):
        Settings()


def test_prod_refuses_to_start_with_the_dev_auth_stub() -> None:
    with pytest.raises(ValidationError, match="dev_header"):
        Settings(env="prod")


@pytest.mark.parametrize(
    "variable",
    ["database.url", "kafka.bootstrap_servers", "kafka.replication_factor", "telemetry.enabled"],
)
def test_prod_refuses_to_fall_back_to_the_local_stack(variable: str) -> None:
    with pytest.raises(ValidationError, match=f"{variable} must be set explicitly"):
        Settings(env="prod")


def test_prod_refuses_to_export_traces_to_the_local_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FINGUARD_TELEMETRY__ENABLED", "true")

    with pytest.raises(ValidationError, match=r"telemetry\.otlp_endpoint must be set explicitly"):
        Settings(env="prod")


def test_prod_without_tracing_needs_no_collector(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINGUARD_TELEMETRY__ENABLED", "false")

    with pytest.raises(ValidationError) as error:
        Settings(env="prod")

    assert "otlp_endpoint" not in str(error.value)


def test_prod_accepts_explicit_connection_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINGUARD_DATABASE__URL", "postgresql+asyncpg://app:pw@db/finguard")
    monkeypatch.setenv("FINGUARD_KAFKA__BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("FINGUARD_KAFKA__REPLICATION_FACTOR", "3")
    monkeypatch.setenv("FINGUARD_TELEMETRY__ENABLED", "true")
    monkeypatch.setenv("FINGUARD_TELEMETRY__OTLP_ENDPOINT", "http://otel-collector:4318")

    with pytest.raises(ValidationError) as error:
        Settings(env="prod")

    assert "must be set explicitly" not in str(error.value)
    assert "dev_header" in str(error.value)


@pytest.mark.parametrize(("env", "docs_enabled"), [("dev", True), ("test", True), ("prod", False)])
def test_api_docs_are_served_outside_prod_only(env: Environment, docs_enabled: bool) -> None:
    # model_construct skips validation: prod has no real authenticator yet.
    settings = Settings.model_construct(env=env)

    assert settings.docs_enabled is docs_enabled


def test_the_database_url_never_appears_in_reprs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINGUARD_DATABASE__URL", "postgresql+asyncpg://app:s3cret@db/finguard")

    settings = Settings(env="test")

    assert settings.database.url.get_secret_value().endswith("@db/finguard")
    assert "s3cret" not in repr(settings)


def test_retry_delays_are_read_as_a_json_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINGUARD_KAFKA__RETRY_DELAYS_S", "[10, 60, 600]")

    assert Settings(env="test").kafka.retry_delays_s == (10.0, 60.0, 600.0)

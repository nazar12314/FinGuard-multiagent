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


@pytest.mark.parametrize(("env", "docs_enabled"), [("dev", True), ("test", True), ("prod", False)])
def test_api_docs_are_served_outside_prod_only(env: Environment, docs_enabled: bool) -> None:
    # model_construct skips validation: prod has no real authenticator yet.
    settings = Settings.model_construct(env=env)

    assert settings.docs_enabled is docs_enabled

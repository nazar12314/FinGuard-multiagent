from collections.abc import Iterator

import pytest

from finguard.core.settings import get_settings
from finguard.entrypoints.api import load_settings_or_exit


@pytest.fixture(autouse=True)
def fresh_settings() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_invalid_config_exits_with_the_env_var_to_fix(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("FINGUARD_API__PORT", "not-a-port")

    with pytest.raises(SystemExit) as exit_info:
        load_settings_or_exit()

    assert exit_info.value.code == 2
    assert "config error: api.port (FINGUARD_API__PORT)" in capsys.readouterr().err


def test_prod_with_the_dev_auth_stub_does_not_start(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("FINGUARD_ENV", "prod")

    with pytest.raises(SystemExit):
        load_settings_or_exit()

    assert "dev_header" in capsys.readouterr().err

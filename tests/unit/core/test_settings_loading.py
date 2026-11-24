from collections.abc import Iterator

import pytest

from finguard.core.settings import get_settings, load_settings_or_exit


@pytest.fixture(autouse=True)
def fresh_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("FINGUARD_ENV", "test")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.parametrize(
    ("variable", "value", "expected"),
    [
        ("FINGUARD_API__PORT", "not-a-port", "config error: api.port (FINGUARD_API__PORT)"),
        ("FINGUARD_API", '"not-a-section"', "config error: api (FINGUARD_API__*)"),
        ("FINGUARD_ENV", None, "config error: env (FINGUARD_ENV): Field required"),
    ],
    ids=["bad-field", "bad-section", "missing-env"],
)
def test_invalid_config_exits_with_the_env_var_to_fix(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    variable: str,
    value: str | None,
    expected: str,
) -> None:
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)

    with pytest.raises(SystemExit) as exit_info:
        load_settings_or_exit()

    assert exit_info.value.code == 2
    assert expected in capsys.readouterr().err


def test_prod_with_the_dev_auth_stub_does_not_start(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("FINGUARD_ENV", "prod")

    with pytest.raises(SystemExit):
        load_settings_or_exit()

    assert "dev_header" in capsys.readouterr().err

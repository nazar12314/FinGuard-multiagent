import configparser
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES_DIR = REPO_ROOT / "src" / "finguard" / "modules"
HEXAGONAL_LAYOUT = ("domain", "ports.py", "service.py", "adapters", "api")


def module_names() -> list[str]:
    return sorted(
        path.name
        for path in MODULES_DIR.iterdir()
        if path.is_dir() and (path / "__init__.py").exists()
    )


def contract_modules(section: str, key: str) -> set[str]:
    config = configparser.ConfigParser()
    config.read(REPO_ROOT / ".importlinter")
    return set(config[f"importlinter:contract:{section}"][key].split())


@pytest.mark.parametrize("module", module_names())
def test_module_follows_hexagonal_layout(module: str) -> None:
    missing = [part for part in HEXAGONAL_LAYOUT if not (MODULES_DIR / module / part).exists()]
    assert not missing, f"{module} is missing {missing}"


@pytest.mark.parametrize(
    ("section", "key"),
    [("module-layers", "containers"), ("module-independence", "modules")],
)
def test_every_module_is_covered_by_import_contracts(section: str, key: str) -> None:
    # A module missing from .importlinter would silently escape boundary enforcement.
    expected = {f"finguard.modules.{name}" for name in module_names()}
    assert contract_modules(section, key) == expected

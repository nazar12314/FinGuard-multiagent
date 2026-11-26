import subprocess
import sys

import pytest


@pytest.mark.parametrize("module", ["finguard.core.http.app", "finguard.entrypoints.outbox_relay"])
def test_processes_other_than_the_watchlist_import_start_without_icu(module: str) -> None:
    # ICU is the screening extra; the API and worker images don't install it.
    program = f"import sys; sys.modules['icu'] = None; import {module}"

    result = subprocess.run([sys.executable, "-c", program], capture_output=True, check=False)

    assert result.returncode == 0, result.stderr.decode()

import signal
from typing import Any

import pytest
import uvicorn

from finguard.core.settings import Settings
from finguard.entrypoints import api


def test_spans_are_flushed_when_sigterm_stops_the_api(monkeypatch: pytest.MonkeyPatch) -> None:
    flushed: list[bool] = []
    monkeypatch.setattr(api, "load_settings_or_exit", lambda: Settings(env="test"))
    monkeypatch.setattr(api, "configure_logging", lambda settings: None)
    monkeypatch.setattr(
        api, "configure_telemetry", lambda settings, process: lambda: flushed.append(True)
    )

    def serve_until_sigterm(app: Any, **options: Any) -> None:
        # What uvicorn does after a graceful shutdown: raise the signal again under the
        # handler that was installed before it started.
        signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr(uvicorn, "run", serve_until_sigterm)
    previous = signal.getsignal(signal.SIGTERM)
    try:
        with pytest.raises(SystemExit) as stopped:
            api.main()
    finally:
        signal.signal(signal.SIGTERM, previous)

    assert stopped.value.code == 128 + signal.SIGTERM
    assert flushed == [True]

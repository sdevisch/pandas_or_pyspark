import os
import importlib

from unipandas.backend import configure_backend, current_backend_name, get_backend, BACKEND_ENV_VAR


def test_configure_backend_pandas(monkeypatch):
    configure_backend("pandas")
    assert current_backend_name() == "pandas"
    assert get_backend().name == "pandas"


def test_env_override_pandas(monkeypatch):
    monkeypatch.setenv(BACKEND_ENV_VAR, "pandas")
    # reload module to trigger detection path if needed
    import unipandas.backend as backend

    importlib.reload(backend)
    assert backend.current_backend_name() == "pandas"

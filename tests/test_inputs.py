import os
import pytest

from MaximaDagster.modules import Inputs


def test_load_inputs_from_env():
    os.environ["MAXIMA_ROOT_DIR"] = "root"
    os.environ["MAXIMA_PONI_FILE"] = "poni"
    os.environ["MAXIMA_PYMCA_CONFIG"] = "cfg"

    cfg = Inputs.load_inputs_from_env()

    assert cfg.root_dir == "root"
    assert cfg.poni_file == "poni"
    assert cfg.config_path == "cfg"


def test_load_inputs_from_env_missing(monkeypatch):
    monkeypatch.delenv("MAXIMA_ROOT_DIR", raising=False)
    monkeypatch.delenv("MAXIMA_PONI_FILE", raising=False)
    monkeypatch.delenv("MAXIMA_PYMCA_CONFIG", raising=False)

    with pytest.raises(ValueError):
        Inputs.load_inputs_from_env()

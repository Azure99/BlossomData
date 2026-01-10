from __future__ import annotations

from pathlib import Path

import pytest

from blossom.conf.config import _discover_config_path, load_config


def test_discover_config_path_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("models: []\n", encoding="utf-8")
    monkeypatch.setenv("BLOSSOM_CONFIG", str(config_path))

    discovered = _discover_config_path("does-not-exist.yaml")
    assert discovered == config_path


def test_discover_config_path_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BLOSSOM_CONFIG", str(tmp_path / "missing.yaml"))

    with pytest.raises(FileNotFoundError) as excinfo:
        _discover_config_path(str(tmp_path / "also-missing.yaml"))

    assert "No configuration file found" in str(excinfo.value)


def test_load_config_yaml_error(tmp_path: Path) -> None:
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("models: [invalid\n", encoding="utf-8")

    with pytest.raises(ValueError) as excinfo:
        load_config(str(bad_yaml))

    assert "Error parsing YAML" in str(excinfo.value)


def test_load_config_validation_error(tmp_path: Path) -> None:
    invalid_cfg = tmp_path / "invalid.yaml"
    invalid_cfg.write_text(
        """models:\n  - name: demo\n    provider: openai\n""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as excinfo:
        load_config(str(invalid_cfg))

    assert "Error validating configuration data" in str(excinfo.value)


def test_load_config_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """models:\n  - name: demo\n    provider: openai\n    config:\n      key: test-key\n""",
        encoding="utf-8",
    )
    monkeypatch.setenv("BLOSSOM_CONFIG", str(cfg))

    config = load_config("ignored.yaml")
    assert config.models[0].name == "demo"
    assert config.models[0].config["key"] == "test-key"

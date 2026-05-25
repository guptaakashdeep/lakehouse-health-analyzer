from pathlib import Path

import pytest

from operator_tui import main


def test_operator_cli_default_routes_to_tui_bootstrap(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "operator_tui._run_textual_tui_bootstrap",
        lambda *, command_name, output, error: calls.append(command_name) or 0,
        raising=False,
    )
    monkeypatch.setattr(
        "operator_tui.AnalyzerConfiguration.from_environment",
        lambda: pytest.fail("default CLI should not use legacy env workflow"),
    )

    assert main([]) == 0
    assert calls == ["lakehouse-health-operator"]


def test_operator_cli_default_shows_setup_needed_when_unconfigured(monkeypatch, capsys):
    monkeypatch.delenv("LHA_METADATA_LOCATION", raising=False)
    monkeypatch.delenv("LHA_GLUE_CATALOG_NAME", raising=False)
    monkeypatch.delenv("LHA_TABLE_SOURCE_KIND", raising=False)
    monkeypatch.setenv("HOME", "/tmp/lha-home-missing-config")

    assert main([]) == 2

    output = capsys.readouterr()
    assert "Setup is required" in output.err
    assert "lakehouse-health-operator setup" in output.err


def test_operator_cli_rejects_removed_legacy_flags(monkeypatch, capsys):
    monkeypatch.setattr(
        "operator_tui._run_textual_tui_bootstrap",
        lambda **_: pytest.fail("removed legacy flags should not start the TUI"),
    )
    monkeypatch.setattr(
        "operator_tui.AnalyzerConfiguration.from_environment",
        lambda: pytest.fail("removed legacy flags should not read configuration"),
    )

    with pytest.raises(SystemExit) as exc_info:
        main(["--refresh"])

    assert exc_info.value.code == 2
    assert "unrecognized arguments: --refresh" in capsys.readouterr().err


def test_operator_cli_scripts_define_short_alias_to_same_entrypoint():
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
        import tomli as tomllib

    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as config_file:
        scripts = tomllib.load(config_file)["project"]["scripts"]

    assert scripts["lakehouse-health-operator"] == "operator_tui:main"
    assert scripts["lh"] == scripts["lakehouse-health-operator"]

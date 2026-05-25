from pathlib import Path

from operator_tui import main


def test_operator_cli_dispatches_setup_subcommand(monkeypatch):
    calls = []

    def fake_run_setup_command(argv):
        calls.append(tuple(argv))
        return 0

    monkeypatch.setattr("operator_tui.run_setup_command", fake_run_setup_command)

    assert main(["setup"]) == 0
    assert calls == [tuple()]


def test_pyproject_exposes_lh_alias_script():
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
        import tomli as tomllib

    pyproject = tomllib.loads(
        Path("pyproject.toml").read_text(encoding="utf-8")
    )

    assert pyproject["project"]["scripts"]["lh"] == "operator_tui:main"

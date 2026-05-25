import sys

from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    OutputPolicy,
)
from operator_tui import main, run_report_command


def test_operator_cli_dispatches_report_subcommand(monkeypatch):
    calls = []

    def fake_run_report_command(argv):
        calls.append(tuple(argv))
        return 23

    monkeypatch.setattr("operator_tui.run_report_command", fake_run_report_command)

    assert main(["report", "sales.orders", "--format", "json"]) == 23
    assert calls == [("sales.orders", "--format", "json")]


def test_report_command_wires_parsed_options_into_workflow_runner(monkeypatch):
    calls = []
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("placeholder",),
            table_name="__placeholder__",
        ),
        output=OutputPolicy(export_directory="/tmp/from-config"),
    )

    monkeypatch.setattr(
        "operator_tui.AnalyzerConfiguration.from_environment",
        lambda: config,
    )
    monkeypatch.setattr(
        "operator_tui.configured_report_command_workflow",
        lambda configured: "workflow",
    )

    def fake_run_report_command_workflow(
        workflow,
        *,
        table_identifier,
        formats,
        output,
        error,
        output_path,
        output_directory,
        configured_output_directory,
        configured_formats,
    ):
        calls.append(
            {
                "workflow": workflow,
                "table_identifier": table_identifier,
                "formats": formats,
                "output": output,
                "error": error,
                "output_path": output_path,
                "output_directory": output_directory,
                "configured_output_directory": configured_output_directory,
                "configured_formats": configured_formats,
            }
        )
        return 0

    monkeypatch.setattr(
        "operator_tui.run_report_command_workflow", fake_run_report_command_workflow
    )

    assert (
        run_report_command(
            [
                "sales.orders",
                "--format",
                "json,markdown",
                "--output-dir",
                "/tmp/from-flag",
            ]
        )
        == 0
    )
    assert len(calls) == 1
    assert calls[0]["workflow"] == "workflow"
    assert calls[0]["table_identifier"] == "sales.orders"
    assert calls[0]["formats"] == ("json", "markdown")
    assert calls[0]["output"] is sys.stdout
    assert calls[0]["error"] is sys.stderr
    assert calls[0]["output_path"] is None
    assert calls[0]["output_directory"] == "/tmp/from-flag"
    assert calls[0]["configured_output_directory"] == "/tmp/from-config"
    assert calls[0]["configured_formats"] == ()


def test_report_command_defaults_to_json_format(monkeypatch):
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("placeholder",),
            table_name="__placeholder__",
        ),
    )
    calls = []

    monkeypatch.setattr(
        "operator_tui.AnalyzerConfiguration.from_environment",
        lambda: config,
    )
    monkeypatch.setattr(
        "operator_tui.configured_report_command_workflow",
        lambda configured: "workflow",
    )

    def fake_run_report_command_workflow(
        workflow,
        *,
        table_identifier,
        formats,
        output,
        error,
        output_path,
        output_directory,
        configured_output_directory,
        configured_formats,
    ):
        calls.append((table_identifier, formats, configured_formats))
        return 0

    monkeypatch.setattr(
        "operator_tui.run_report_command_workflow", fake_run_report_command_workflow
    )

    assert run_report_command(["sales.orders"]) == 0
    assert calls == [("sales.orders", ("json",), ())]


def test_report_command_uses_configured_formats_when_format_flag_is_omitted(
    monkeypatch,
):
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("placeholder",),
            table_name="__placeholder__",
        ),
        output=OutputPolicy(export_formats=("markdown",)),
    )
    calls = []

    monkeypatch.setattr(
        "operator_tui.AnalyzerConfiguration.from_environment",
        lambda: config,
    )
    monkeypatch.setattr(
        "operator_tui.configured_report_command_workflow",
        lambda configured: "workflow",
    )

    def fake_run_report_command_workflow(
        workflow,
        *,
        table_identifier,
        formats,
        output,
        error,
        output_path,
        output_directory,
        configured_output_directory,
        configured_formats,
    ):
        calls.append((table_identifier, formats, configured_formats))
        return 0

    monkeypatch.setattr(
        "operator_tui.run_report_command_workflow", fake_run_report_command_workflow
    )

    assert run_report_command(["sales.orders"]) == 0
    assert calls == [("sales.orders", ("markdown",), ("markdown",))]

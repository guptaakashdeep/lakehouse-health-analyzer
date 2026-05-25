from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Mapping, Sequence, TextIO

from analysis.report import TableHealthReport
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from workflows.report_command import configured_report_command_workflow
from workflows.report_command import parse_report_formats
from workflows.report_command import run_report_command_workflow


def run_setup_command(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-health-operator setup",
        description="Configure AWS Glue defaults and write config.toml.",
    )
    parser.parse_args(argv)

    from rich.console import Console
    from workflows.setup import (
        RichSetupPrompter,
        default_aws_config_path,
        default_setup_config_path,
        run_setup_workflow,
        validate_glue_catalog_selection,
    )

    console = Console()
    run_setup_workflow(
        config_path=default_setup_config_path(dict(os.environ)),
        aws_config_path=default_aws_config_path(dict(os.environ)),
        prompter=RichSetupPrompter(console=console),
        validate_selection=validate_glue_catalog_selection,
        emit_message=console.print,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    command_argv = list(argv) if argv is not None else sys.argv[1:]
    if command_argv and command_argv[0] == "setup":
        return run_setup_command(command_argv[1:])
    if command_argv and command_argv[0] == "report":
        return run_report_command(command_argv[1:])

    parser = argparse.ArgumentParser(
        prog="lakehouse-health-operator",
        description="Launch the Lakehouse Health Analyzer operator TUI.",
    )
    parser.parse_args(command_argv)

    return _run_textual_tui_bootstrap(
        command_name=parser.prog,
        output=sys.stdout,
        error=sys.stderr,
    )


def run_report_command(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-health-operator report",
        description="Render fresh table health reports in JSON or Markdown.",
    )
    parser.add_argument(
        "table_identifier",
        metavar="NAMESPACE.TABLE",
        help=(
            "Namespace-qualified table identifier. "
            "Use dots for namespace segments (for example: sales.orders)."
        ),
    )
    parser.add_argument(
        "--format",
        default=None,
        help="Output format, or comma-separated formats (json, markdown).",
    )
    parser.add_argument(
        "--output",
        help="Exact output file path for a single format export.",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for generated report file names.",
    )
    args = parser.parse_args(argv)

    config = AnalyzerConfiguration.from_environment()
    workflow = configured_report_command_workflow(config)
    formats = parse_report_formats(args.format)
    if not formats:
        formats = config.output.export_formats or ("json",)
    return run_report_command_workflow(
        workflow,
        table_identifier=args.table_identifier,
        formats=formats,
        output=sys.stdout,
        error=sys.stderr,
        output_path=args.output,
        output_directory=args.output_dir,
        configured_output_directory=config.output.export_directory,
        configured_formats=config.output.export_formats,
    )


def _run_textual_tui_bootstrap(
    *, command_name: str, output: TextIO, error: TextIO
) -> int:
    if not _operator_setup_is_configured():
        error.write(_setup_needed_message(command_name))
        error.write("\n")
        return 2
    from .app import configured_operator_catalog_browser_app

    try:
        app = configured_operator_catalog_browser_app(
            AnalyzerConfiguration.from_environment()
        )
    except (KeyError, ValueError) as exc:
        error.write(_setup_needed_message(command_name))
        error.write("\n")
        error.write(str(exc))
        error.write("\n")
        return 2
    app.run()
    return 0


def _glue_catalog_properties(
    source: GlueCatalogTableSourceConfiguration,
) -> Mapping[str, str]:
    properties = {"type": "glue"}
    if source.aws_profile is not None:
        properties["glue.profile-name"] = source.aws_profile
    if source.region is not None:
        properties["glue.region"] = source.region
    return properties


def _analyze_iceberg_table(config: AnalyzerConfiguration) -> TableHealthReport:
    from analysis.iceberg import analyze_iceberg_table

    return analyze_iceberg_table(config)


def _operator_setup_is_configured(
    environ: Mapping[str, str] | None = None,
) -> bool:
    values = os.environ if environ is None else environ
    if _operator_config_path(values).exists():
        return True
    if values.get("LHA_METADATA_LOCATION"):
        return True
    if values.get("LHA_GLUE_CATALOG_NAME"):
        return True
    return False


def _operator_config_path(environ: Mapping[str, str]) -> Path:
    config_home = environ.get("XDG_CONFIG_HOME")
    if not config_home:
        config_home = str(Path.home() / ".config")
    return Path(config_home) / "lakehouse-health-analyzer" / "config.toml"


def _setup_needed_message(command_name: str) -> str:
    return (
        "Setup is required before launching the operator TUI.\n"
        f"Run `{command_name} setup` or `lh setup` to configure access."
    )

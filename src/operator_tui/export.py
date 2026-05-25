from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from analysis.report import TableHealthReport
from configuration import OutputPolicy
from report_exports import export_table_health_report_for_output_policy


@dataclass(frozen=True)
class TableReportExportWorkflow:
    output_policy: OutputPolicy
    resolve_cwd: Callable[[], Path] = Path.cwd

    def export_offer(self, *, table_name: str) -> str:
        destination = self._destination_directory()
        return "\n".join(
            (
                f"Export report for {table_name}",
                f"Destination: {destination}",
                "",
                "Choose format:",
                "1 JSON",
                "2 Markdown",
                "3 Both",
            )
        )

    def export_report(
        self,
        report: TableHealthReport,
        *,
        export_choice: str,
        cache_status: str,
        analyzed_at: datetime,
    ) -> tuple[Path, ...]:
        export_formats = _export_formats_for_choice(export_choice)
        output_policy = OutputPolicy(
            export_formats=export_formats,
            export_directory=str(self._destination_directory()),
        )
        return export_table_health_report_for_output_policy(
            report,
            output_policy=output_policy,
            cache_status=cache_status,
            analyzed_at=analyzed_at,
        )

    def _destination_directory(self) -> Path:
        if self.output_policy.export_directory:
            return Path(self.output_policy.export_directory)
        return self.resolve_cwd()


def _export_formats_for_choice(export_choice: str) -> tuple[str, ...]:
    normalized = export_choice.strip().lower()
    if normalized == "json":
        return ("json",)
    if normalized == "markdown":
        return ("markdown",)
    if normalized == "both":
        return ("json", "markdown")
    raise ValueError(f"Unsupported export choice: {export_choice}")

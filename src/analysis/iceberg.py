from typing import Any, Iterable, Mapping, Optional, Tuple

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    TableHealthReport,
    TableSource,
)


def analyze_iceberg_metadata_file(metadata_location: str) -> TableHealthReport:
    table = _load_static_table(metadata_location)
    return IcebergTableFormatAdapter().analyze_table(
        table=table,
        table_source=TableSource(kind="metadata_file", location=metadata_location),
    )


class IcebergTableFormatAdapter:
    def analyze_table(self, table: Any, table_source: TableSource) -> TableHealthReport:
        table_name = _table_name(table)
        file_rows = tuple(_rows_from_table(table.inspect.files()))

        data_file_count = sum(1 for row in file_rows if _content_code(row) == 0)
        delete_file_count = sum(1 for row in file_rows if _content_code(row) != 0)
        total_file_size_bytes = _sum_all_known(
            row.get("file_size_in_bytes") for row in file_rows
        )
        data_file_size_bytes = _sum_all_known(
            row.get("file_size_in_bytes")
            for row in file_rows
            if _content_code(row) == 0
        )
        warnings = []
        if total_file_size_bytes is None:
            warnings.append(
                CalculationWarning(
                    metric_key="total_file_size_bytes",
                    message=(
                        "Iceberg file metadata did not include file sizes, so total "
                        "file size is unknown."
                    ),
                )
            )
        if data_file_count > 0 and data_file_size_bytes is None:
            warnings.append(
                CalculationWarning(
                    metric_key="data_file_size_bytes",
                    message=(
                        "Iceberg file metadata did not include every data file size, "
                        "so data file size is unknown."
                    ),
                )
            )

        health_metrics = (
            HealthMetric(
                key="data_file_count",
                label="Data File Count",
                value=data_file_count,
                unit="files",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="delete_file_count",
                label="Delete File Count",
                value=delete_file_count,
                unit="files",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="total_file_size_bytes",
                label="Total File Size",
                value=total_file_size_bytes,
                unit="bytes",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="data_file_size_bytes",
                label="Data File Size",
                value=data_file_size_bytes,
                unit="bytes",
                source="iceberg.inspect.files",
            ),
        )

        display_statistics = (
            DisplayStatistic(
                key="total_file_size",
                label="Total File Size",
                value=_format_bytes(total_file_size_bytes),
                derived_from=("total_file_size_bytes",),
            ),
            DisplayStatistic(
                key="average_data_file_size",
                label="Average Data File Size",
                value=_format_bytes(_average(data_file_size_bytes, data_file_count)),
                derived_from=("data_file_size_bytes", "data_file_count"),
            ),
        )

        return TableHealthReport(
            table_name=table_name,
            table_source=table_source,
            health_metrics=health_metrics,
            display_statistics=display_statistics,
            calculation_warnings=tuple(warnings),
        )


def _load_static_table(metadata_location: str) -> Any:
    from pyiceberg.table import StaticTable

    return StaticTable.from_metadata(metadata_location=metadata_location)


def _table_name(table: Any) -> str:
    name = table.name()
    if isinstance(name, str):
        return name
    return ".".join(str(part) for part in name)


def _rows_from_table(files: Any) -> Iterable[Mapping[str, Any]]:
    if hasattr(files, "to_pylist"):
        return files.to_pylist()
    if hasattr(files, "to_pandas"):
        return files.to_pandas().to_dict("records")
    return files


def _content_code(row: Mapping[str, Any]) -> int:
    content = row.get("content")
    if hasattr(content, "value"):
        return int(content.value)
    return int(content)


def _sum_all_known(values: Iterable[Optional[int]]) -> Optional[int]:
    total = 0
    found = False
    for value in values:
        if value is None:
            return None
        total += int(value)
        found = True
    return total if found else None


def _average(total: Optional[int], count: int) -> Optional[float]:
    if total is None or count == 0:
        return None
    return total / count


def _format_bytes(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if value < 1024:
        return f"{value:g} B"
    return f"{value / 1024:g} KiB"

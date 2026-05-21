from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Tuple

from pyiceberg.catalog import load_catalog

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    PartitionHealthMetric,
    TableHealthReport,
    TableSource,
)
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration


HIGH_FILE_COUNT_PARTITION_THRESHOLD = 100


def analyze_iceberg_metadata_file(
    metadata_source: str | AnalyzerConfiguration,
) -> TableHealthReport:
    metadata_location = _metadata_location(metadata_source)
    snapshot_retention_days = _snapshot_retention_days(metadata_source)
    table = _load_static_table(metadata_location)
    return IcebergTableFormatAdapter(
        snapshot_retention_days=snapshot_retention_days
    ).analyze_table(
        table=table,
        table_source=TableSource(kind="metadata_file", location=metadata_location),
    )


def analyze_iceberg_table(config: AnalyzerConfiguration) -> TableHealthReport:
    table_source = config.table_source
    if isinstance(table_source, GlueCatalogTableSourceConfiguration):
        table = _load_glue_catalog_table(table_source)
        return IcebergTableFormatAdapter(
            snapshot_retention_days=config.analysis.snapshot_retention_days
        ).analyze_table(
            table=table,
            table_source=TableSource(
                kind=table_source.kind,
                location=_catalog_table_location(table_source),
            ),
        )

    return analyze_iceberg_metadata_file(config)


class IcebergTableFormatAdapter:
    def __init__(self, snapshot_retention_days: int = 30):
        self.snapshot_retention_days = snapshot_retention_days

    def analyze_table(self, table: Any, table_source: TableSource) -> TableHealthReport:
        table_name = _table_name(table)
        file_rows = tuple(_rows_from_table(table.inspect.files()))

        data_file_count = sum(1 for row in file_rows if _content_code(row) == 0)
        position_delete_file_count = sum(
            1 for row in file_rows if _content_code(row) == 1
        )
        equality_delete_file_count = sum(
            1 for row in file_rows if _content_code(row) == 2
        )
        delete_file_count = position_delete_file_count + equality_delete_file_count
        total_file_size_bytes = _sum_all_known(
            row.get("file_size_in_bytes") for row in file_rows
        )
        data_file_size_bytes = _sum_all_known(
            row.get("file_size_in_bytes")
            for row in file_rows
            if _content_code(row) == 0
        )
        delete_file_rows = tuple(
            row for row in file_rows if _content_code(row) in (1, 2)
        )
        delete_file_size_bytes = _sum_matching_sizes(delete_file_rows)
        data_file_record_count = _sum_all_known(
            row.get("record_count") for row in file_rows if _content_code(row) == 0
        )
        position_delete_record_count = _sum_record_count_for_content(
            file_rows, content_code=1
        )
        equality_delete_record_count = _sum_record_count_for_content(
            file_rows, content_code=2
        )
        has_delete_files = delete_file_count > 0
        estimated_current_record_count = (
            data_file_record_count if not has_delete_files else None
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
        if delete_file_count > 0 and delete_file_size_bytes is None:
            warnings.append(
                CalculationWarning(
                    metric_key="delete_file_size_bytes",
                    message=(
                        "Iceberg file metadata did not include every delete file size, "
                        "so delete file size is unknown."
                    ),
                )
            )
        if has_delete_files:
            warnings.append(
                CalculationWarning(
                    metric_key="estimated_current_record_count",
                    message=(
                        "Delete files are present, so current records cannot be "
                        "calculated exactly from metadata-only file counts."
                    ),
                )
            )
        partition_metrics = _partition_metrics(table)
        partition_count = len(partition_metrics)
        partition_data_file_counts = tuple(
            metric.data_file_count for metric in partition_metrics
        )
        partition_sizes = tuple(
            metric.total_data_file_size_bytes for metric in partition_metrics
        )
        partition_total_data_file_size_bytes = (
            0 if partition_count == 0 else _sum_all_known(partition_sizes)
        )
        partition_size_skewness = (
            0
            if partition_count == 0
            else _coefficient_of_variation_when_all_known(partition_sizes)
        )
        if partition_count > 0 and partition_size_skewness is None:
            warnings.append(
                CalculationWarning(
                    metric_key="partition_size_skewness",
                    message=(
                        "Iceberg partition metadata did not include every partition "
                        "size, so partition size skewness is unknown."
                    ),
                )
            )
        snapshot_metrics, snapshot_warnings = _snapshot_metrics(
            table=table,
            snapshot_retention_days=self.snapshot_retention_days,
        )
        warnings.extend(snapshot_warnings)

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
            HealthMetric(
                key="delete_file_size_bytes",
                label="Delete File Size",
                value=delete_file_size_bytes,
                unit="bytes",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="data_file_record_count",
                label="Data File Record Count",
                value=data_file_record_count,
                unit="records",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="position_delete_record_count",
                label="Position Delete Record Count",
                value=position_delete_record_count,
                unit="records",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="equality_delete_record_count",
                label="Equality Delete Record Count",
                value=equality_delete_record_count,
                unit="records",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="estimated_current_record_count",
                label="Estimated Current Record Count",
                value=estimated_current_record_count,
                unit="records",
                source="iceberg.inspect.files",
            ),
            HealthMetric(
                key="partition_count",
                label="Partition Count",
                value=partition_count,
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="average_data_files_per_partition",
                label="Average Data Files Per Partition",
                value=_average(sum(partition_data_file_counts), partition_count),
                unit="files/partition",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="max_data_files_per_partition",
                label="Max Data Files Per Partition",
                value=max(partition_data_file_counts, default=0),
                unit="files",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="high_file_count_partition_count",
                label="High File Count Partition Count",
                value=sum(
                    1
                    for file_count in partition_data_file_counts
                    if file_count > HIGH_FILE_COUNT_PARTITION_THRESHOLD
                ),
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="high_file_count_partition_threshold",
                label="High File Count Partition Threshold",
                value=HIGH_FILE_COUNT_PARTITION_THRESHOLD,
                unit="files",
                source="analysis.policy.default",
            ),
            HealthMetric(
                key="partition_total_data_file_size_bytes",
                label="Partition Total Data File Size",
                value=partition_total_data_file_size_bytes,
                unit="bytes",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="average_partition_data_file_size_bytes",
                label="Average Partition Data File Size",
                value=_average(partition_total_data_file_size_bytes, partition_count),
                unit="bytes",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="min_partition_data_file_size_bytes",
                label="Min Partition Data File Size",
                value=_min_all_known(partition_sizes),
                unit="bytes",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="max_partition_data_file_size_bytes",
                label="Max Partition Data File Size",
                value=_max_all_known(partition_sizes),
                unit="bytes",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="partition_size_skewness",
                label="Partition Size Skewness",
                value=partition_size_skewness,
                unit=None,
                source="iceberg.inspect.partitions",
            ),
            *snapshot_metrics,
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
            partition_metrics=partition_metrics,
        )


def _load_static_table(metadata_location: str) -> Any:
    from pyiceberg.table import StaticTable

    return StaticTable.from_metadata(metadata_location=metadata_location)


def _load_glue_catalog_table(source: GlueCatalogTableSourceConfiguration) -> Any:
    properties = {"type": "glue"}
    if source.aws_profile is not None:
        properties["glue.profile-name"] = source.aws_profile
    if source.region is not None:
        properties["glue.region"] = source.region

    catalog = load_catalog(source.catalog_name, **properties)
    return catalog.load_table((*source.namespace, source.table_name))


def _catalog_table_location(source: GlueCatalogTableSourceConfiguration) -> str:
    return ".".join((source.catalog_name, *source.namespace, source.table_name))


def _metadata_location(metadata_source: str | AnalyzerConfiguration) -> str:
    if isinstance(metadata_source, AnalyzerConfiguration):
        return metadata_source.table_source.location
    return metadata_source


def _snapshot_retention_days(metadata_source: str | AnalyzerConfiguration) -> int:
    if isinstance(metadata_source, AnalyzerConfiguration):
        return metadata_source.analysis.snapshot_retention_days
    return 30


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


def _partition_metrics(table: Any) -> Tuple[PartitionHealthMetric, ...]:
    if not hasattr(table.inspect, "partitions"):
        return ()

    return tuple(
        _partition_metric_from_row(row)
        for row in _rows_from_table(table.inspect.partitions())
    )


def _partition_metric_from_row(row: Mapping[str, Any]) -> PartitionHealthMetric:
    data_file_count = int(row.get("file_count") or row.get("data_file_count") or 0)
    total_data_file_size_bytes = row.get(
        "total_data_file_size_in_bytes", row.get("total_data_file_size")
    )

    return PartitionHealthMetric(
        partition=_partition_value(row),
        data_file_count=data_file_count,
        delete_file_count=int(row.get("position_delete_file_count") or 0)
        + int(row.get("equality_delete_file_count") or 0),
        total_data_file_size_bytes=total_data_file_size_bytes,
        average_data_file_size_bytes=_average(
            total_data_file_size_bytes, data_file_count
        ),
        source="iceberg.inspect.partitions",
    )


def _snapshot_metrics(
    table: Any, snapshot_retention_days: int
) -> tuple[tuple[HealthMetric, ...], tuple[CalculationWarning, ...]]:
    if not hasattr(table.inspect, "snapshots"):
        return _unknown_snapshot_metrics(
            snapshot_retention_days,
            "Iceberg snapshot metadata was not available, so snapshot health "
            "metrics are unknown.",
        )

    snapshot_rows = tuple(_rows_from_table(table.inspect.snapshots()))
    if any(_snapshot_id(row) is None for row in snapshot_rows):
        return _unknown_snapshot_metrics(
            snapshot_retention_days,
            "Iceberg snapshot metadata did not include every snapshot id, so "
            "snapshot health metrics are unknown.",
        )

    snapshot_rows_with_ages = tuple(
        (row, _snapshot_age_days(row)) for row in snapshot_rows
    )
    if any(age_days is None for _, age_days in snapshot_rows_with_ages):
        return _unknown_snapshot_metrics(
            snapshot_retention_days,
            "Iceberg snapshot metadata did not include every snapshot timestamp, "
            "so snapshot health metrics are unknown.",
        )

    snapshot_ages = tuple(
        age_days for _, age_days in snapshot_rows_with_ages if age_days is not None
    )
    current_snapshot_id = _current_snapshot_id(table)
    if snapshot_rows and current_snapshot_id is None:
        return (
            _snapshot_health_metrics(
                valid_snapshot_count=len(snapshot_ages),
                oldest_snapshot_age_days=max(snapshot_ages, default=None),
                latest_snapshot_age_days=min(snapshot_ages, default=None),
                snapshot_retention_days=snapshot_retention_days,
                expirable_snapshot_candidate_count=None,
            ),
            (
                CalculationWarning(
                    metric_key="expirable_snapshot_candidate_count",
                    message=(
                        "Iceberg current snapshot metadata was not available, so "
                        "Expirable Snapshot Candidates cannot be calculated safely."
                    ),
                ),
            ),
        )

    expirable_candidate_count = sum(
        1
        for row, age_days in snapshot_rows_with_ages
        if age_days is not None
        and age_days >= snapshot_retention_days
        and _snapshot_id(row) != current_snapshot_id
    )

    return (
        _snapshot_health_metrics(
            valid_snapshot_count=len(snapshot_ages),
            oldest_snapshot_age_days=max(snapshot_ages, default=None),
            latest_snapshot_age_days=min(snapshot_ages, default=None),
            snapshot_retention_days=snapshot_retention_days,
            expirable_snapshot_candidate_count=expirable_candidate_count,
        ),
        (),
    )


def _snapshot_health_metrics(
    valid_snapshot_count: Optional[int],
    oldest_snapshot_age_days: Optional[int],
    latest_snapshot_age_days: Optional[int],
    snapshot_retention_days: int,
    expirable_snapshot_candidate_count: Optional[int],
) -> tuple[HealthMetric, ...]:
    return (
        HealthMetric(
            key="valid_snapshot_count",
            label="Valid Snapshot Count",
            value=valid_snapshot_count,
            unit="snapshots",
            source="iceberg.inspect.snapshots",
        ),
        HealthMetric(
            key="oldest_snapshot_age_days",
            label="Oldest Snapshot Age",
            value=oldest_snapshot_age_days,
            unit="days",
            source="iceberg.inspect.snapshots",
        ),
        HealthMetric(
            key="latest_snapshot_age_days",
            label="Latest Snapshot Age",
            value=latest_snapshot_age_days,
            unit="days",
            source="iceberg.inspect.snapshots",
        ),
        HealthMetric(
            key="snapshot_retention_days",
            label="Snapshot Retention",
            value=snapshot_retention_days,
            unit="days",
            source="analysis.policy.snapshot_retention_days",
        ),
        HealthMetric(
            key="expirable_snapshot_candidate_count",
            label="Expirable Snapshot Candidate Count",
            value=expirable_snapshot_candidate_count,
            unit="snapshots",
            source="iceberg.inspect.snapshots",
        ),
    )


def _unknown_snapshot_metrics(
    snapshot_retention_days: int, message: str
) -> tuple[tuple[HealthMetric, ...], tuple[CalculationWarning, ...]]:
    unknown_metric_keys = (
        "valid_snapshot_count",
        "oldest_snapshot_age_days",
        "latest_snapshot_age_days",
        "expirable_snapshot_candidate_count",
    )
    return (
        _snapshot_health_metrics(
            valid_snapshot_count=None,
            oldest_snapshot_age_days=None,
            latest_snapshot_age_days=None,
            snapshot_retention_days=snapshot_retention_days,
            expirable_snapshot_candidate_count=None,
        ),
        tuple(
            CalculationWarning(metric_key=metric_key, message=message)
            for metric_key in unknown_metric_keys
        ),
    )


def _snapshot_age_days(row: Mapping[str, Any]) -> Optional[int]:
    committed_at = _snapshot_committed_at(row)
    if committed_at is None:
        return None
    return int((_utc_now() - committed_at).total_seconds() // 86400)


def _snapshot_committed_at(row: Mapping[str, Any]) -> Optional[datetime]:
    value = row.get("committed_at", row.get("committed-at"))
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    timestamp_ms = row.get("timestamp_ms", row.get("timestamp-ms"))
    if timestamp_ms is not None:
        return datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
    return None


def _current_snapshot_id(table: Any) -> Any:
    if not hasattr(table, "current_snapshot"):
        return None
    current_snapshot = table.current_snapshot()
    if isinstance(current_snapshot, Mapping):
        return _snapshot_id(current_snapshot)
    snapshot_id = getattr(current_snapshot, "snapshot_id", None)
    if callable(snapshot_id):
        return snapshot_id()
    return snapshot_id


def _snapshot_id(row: Mapping[str, Any]) -> Any:
    return row.get("snapshot_id", row.get("snapshot-id"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _partition_value(row: Mapping[str, Any]) -> Mapping[str, Any]:
    partition = row.get("partition", {})
    if partition is None:
        return {}
    if isinstance(partition, Mapping):
        return partition
    if hasattr(partition, "as_dict"):
        return partition.as_dict()
    return {"partition": partition}


def _sum_all_known(values: Iterable[Optional[int]]) -> Optional[int]:
    total = 0
    found = False
    for value in values:
        if value is None:
            return None
        total += int(value)
        found = True
    return total if found else None


def _min_all_known(values: Iterable[Optional[int]]) -> Optional[int]:
    known_values = tuple(values)
    if not known_values or any(value is None for value in known_values):
        return None
    return min(int(value) for value in known_values if value is not None)


def _max_all_known(values: Iterable[Optional[int]]) -> Optional[int]:
    known_values = tuple(values)
    if not known_values or any(value is None for value in known_values):
        return None
    return max(int(value) for value in known_values if value is not None)


def _sum_record_count_for_content(
    rows: Iterable[Mapping[str, Any]], content_code: int
) -> Optional[int]:
    matching_rows = tuple(row for row in rows if _content_code(row) == content_code)
    if not matching_rows:
        return 0
    return _sum_all_known(row.get("record_count") for row in matching_rows)


def _sum_matching_sizes(rows: Iterable[Mapping[str, Any]]) -> Optional[int]:
    matching_rows = tuple(rows)
    if not matching_rows:
        return 0
    return _sum_all_known(row.get("file_size_in_bytes") for row in matching_rows)


def _average(total: Optional[int], count: int) -> Optional[float]:
    if total is None or count == 0:
        return None
    return total / count


def _coefficient_of_variation_when_all_known(
    values: Iterable[Optional[int]],
) -> Optional[float]:
    raw_values = tuple(values)
    if any(value is None for value in raw_values):
        return None
    known_values = tuple(float(value) for value in raw_values)
    if not known_values:
        return None

    mean = sum(known_values) / len(known_values)
    if mean == 0:
        return 0

    variance = sum((value - mean) ** 2 for value in known_values) / len(known_values)
    return variance**0.5 / mean


def _format_bytes(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if value < 1024:
        return f"{value:g} B"
    return f"{value / 1024:g} KiB"

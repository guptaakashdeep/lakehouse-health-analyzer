from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Tuple

from pyiceberg.catalog import load_catalog

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    EvolutionChange,
    HealthMetric,
    MaintenanceRecommendation,
    PartitionHealthMetric,
    TableEvolutionHistory,
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
    recommendation_thresholds = _recommendation_thresholds(metadata_source)
    table = _load_static_table(metadata_location)
    return IcebergTableFormatAdapter(
        snapshot_retention_days=snapshot_retention_days,
        recommendation_thresholds=recommendation_thresholds,
    ).analyze_table(
        table=table,
        table_source=TableSource(kind="metadata_file", location=metadata_location),
    )


def analyze_iceberg_table(config: AnalyzerConfiguration) -> TableHealthReport:
    table_source = config.table_source
    if isinstance(table_source, GlueCatalogTableSourceConfiguration):
        table = _load_glue_catalog_table(table_source)
        return IcebergTableFormatAdapter(
            snapshot_retention_days=config.analysis.snapshot_retention_days,
            recommendation_thresholds=config.analysis.recommendation_thresholds,
        ).analyze_table(
            table=table,
            table_source=TableSource(
                kind=table_source.kind,
                location=_catalog_table_location(table_source),
            ),
        )

    return analyze_iceberg_metadata_file(config)


class IcebergTableFormatAdapter:
    def __init__(
        self,
        snapshot_retention_days: int = 30,
        recommendation_thresholds: Mapping[str, int | float] | None = None,
    ):
        self.snapshot_retention_days = snapshot_retention_days
        self.recommendation_thresholds = (
            {} if recommendation_thresholds is None else recommendation_thresholds
        )

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
        table_evolution_history, table_evolution_warnings = _table_evolution_history(
            table
        )
        warnings.extend(table_evolution_warnings)

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
        maintenance_recommendations = _maintenance_recommendations(
            health_metrics=health_metrics,
            thresholds=self.recommendation_thresholds,
        )

        return TableHealthReport(
            table_name=table_name,
            table_source=table_source,
            health_metrics=health_metrics,
            display_statistics=display_statistics,
            calculation_warnings=tuple(warnings),
            partition_metrics=partition_metrics,
            table_evolution_history=table_evolution_history,
            maintenance_recommendations=maintenance_recommendations,
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


def _recommendation_thresholds(
    metadata_source: str | AnalyzerConfiguration,
) -> Mapping[str, int | float]:
    if isinstance(metadata_source, AnalyzerConfiguration):
        return metadata_source.analysis.recommendation_thresholds
    return {}


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


def _maintenance_recommendations(
    health_metrics: tuple[HealthMetric, ...],
    thresholds: Mapping[str, int | float],
) -> tuple[MaintenanceRecommendation, ...]:
    metric_values = {metric.key: metric.value for metric in health_metrics}
    recommendations = []

    high_partition_count = metric_values.get("high_file_count_partition_count")
    high_partition_count_warning = _threshold_value(
        thresholds, "high_file_count_partition_count_warning", 1
    )
    high_partition_count_critical = _threshold_value(
        thresholds, "high_file_count_partition_count_critical", 5
    )
    high_file_count_threshold = metric_values.get("high_file_count_partition_threshold")
    if (
        high_partition_count is not None
        and high_file_count_threshold is not None
        and (
            high_partition_count >= high_partition_count_warning
            or high_partition_count >= high_partition_count_critical
        )
    ):
        severity = (
            "critical"
            if high_partition_count >= high_partition_count_critical
            else "warning"
        )
        recommendations.append(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity=severity,
                evidence={
                    "high_file_count_partition_count": high_partition_count,
                    "max_data_files_per_partition": metric_values.get(
                        "max_data_files_per_partition"
                    ),
                },
                thresholds={
                    "high_file_count_partition_threshold": high_file_count_threshold,
                    "high_file_count_partition_count_warning": (
                        high_partition_count_warning
                    ),
                    "high_file_count_partition_count_critical": (
                        high_partition_count_critical
                    ),
                },
                rationale=(
                    "Compact data files in high file-count partitions to reduce "
                    "planning overhead and read amplification."
                ),
            )
        )

    expirable_snapshot_candidate_count = metric_values.get(
        "expirable_snapshot_candidate_count"
    )
    expirable_snapshot_count_info = _threshold_value(
        thresholds, "expirable_snapshot_candidate_count_info", 1
    )
    if (
        expirable_snapshot_candidate_count is not None
        and expirable_snapshot_candidate_count >= expirable_snapshot_count_info
    ):
        recommendations.append(
            MaintenanceRecommendation(
                recommendation_type="snapshot_expiration",
                severity="info",
                evidence={
                    "expirable_snapshot_candidate_count": (
                        expirable_snapshot_candidate_count
                    ),
                    "oldest_snapshot_age_days": metric_values.get(
                        "oldest_snapshot_age_days"
                    ),
                },
                thresholds={
                    "snapshot_retention_days": metric_values.get(
                        "snapshot_retention_days"
                    ),
                    "expirable_snapshot_candidate_count_info": (
                        expirable_snapshot_count_info
                    ),
                },
                rationale=(
                    "Expire eligible retained snapshots under the configured "
                    "retention policy to reduce retained metadata and storage "
                    "pressure."
                ),
            )
        )

    valid_snapshot_count = metric_values.get("valid_snapshot_count")
    valid_snapshot_count_warning = _threshold_value(
        thresholds, "valid_snapshot_count_warning", 100
    )
    valid_snapshot_count_critical = _threshold_value(
        thresholds, "valid_snapshot_count_critical", 500
    )
    if valid_snapshot_count is not None and (
        valid_snapshot_count >= valid_snapshot_count_warning
        or valid_snapshot_count >= valid_snapshot_count_critical
    ):
        severity = (
            "critical"
            if valid_snapshot_count >= valid_snapshot_count_critical
            else "warning"
        )
        recommendations.append(
            MaintenanceRecommendation(
                recommendation_type="metadata_cleanup",
                severity=severity,
                evidence={
                    "valid_snapshot_count": valid_snapshot_count,
                    "oldest_snapshot_age_days": metric_values.get(
                        "oldest_snapshot_age_days"
                    ),
                },
                thresholds={
                    "valid_snapshot_count_warning": valid_snapshot_count_warning,
                    "valid_snapshot_count_critical": valid_snapshot_count_critical,
                },
                rationale=(
                    "Review metadata history retention because many valid "
                    "snapshots are retained for this table."
                ),
            )
        )

    delete_file_count = metric_values.get("delete_file_count")
    delete_file_count_info = _threshold_value(thresholds, "delete_file_count_info", 1)
    delete_file_count_warning = _threshold_value(
        thresholds, "delete_file_count_warning", 10
    )
    delete_file_count_critical = _threshold_value(
        thresholds, "delete_file_count_critical", 100
    )
    if delete_file_count is not None and (
        delete_file_count >= delete_file_count_info
        or delete_file_count >= delete_file_count_warning
        or delete_file_count >= delete_file_count_critical
    ):
        severity = "info"
        if delete_file_count >= delete_file_count_critical:
            severity = "critical"
        elif delete_file_count >= delete_file_count_warning:
            severity = "warning"
        recommendations.append(
            MaintenanceRecommendation(
                recommendation_type="delete_file_cleanup",
                severity=severity,
                evidence={
                    "delete_file_count": delete_file_count,
                    "position_delete_record_count": metric_values.get(
                        "position_delete_record_count"
                    ),
                    "equality_delete_record_count": metric_values.get(
                        "equality_delete_record_count"
                    ),
                    "delete_file_size_bytes": metric_values.get(
                        "delete_file_size_bytes"
                    ),
                },
                thresholds={
                    "delete_file_count_info": delete_file_count_info,
                    "delete_file_count_warning": delete_file_count_warning,
                    "delete_file_count_critical": delete_file_count_critical,
                },
                rationale=(
                    "Review delete file pressure because delete files can increase "
                    "read planning and scan work."
                ),
            )
        )

    return tuple(recommendations)


def _threshold_value(
    thresholds: Mapping[str, int | float], key: str, default: int | float
) -> int | float:
    return thresholds.get(key, default)


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


def _table_evolution_history(
    table: Any,
) -> tuple[TableEvolutionHistory, tuple[CalculationWarning, ...]]:
    metadata_entries = _retained_metadata_entries(table)
    schema_changes = []
    property_changes = []
    warnings = []

    if _retained_metadata_history_complete(
        table
    ) is False or _has_uninspected_metadata_log(table):
        warnings.append(
            CalculationWarning(
                metric_key="table_evolution_history",
                message=(
                    "Iceberg retained metadata history is incomplete or pruned, so "
                    "Table Evolution History is not a complete audit log."
                ),
            )
        )

    for previous, current in zip(metadata_entries, metadata_entries[1:]):
        schema_changes.extend(_schema_changes(previous, current))
        property_changes.extend(_property_changes(previous, current))

    return (
        TableEvolutionHistory(
            schema_changes=tuple(schema_changes),
            property_changes=tuple(property_changes),
        ),
        tuple(warnings),
    )


def _retained_metadata_entries(table: Any) -> tuple[Any, ...]:
    metadata_history = _attribute_or_call(table, "metadata_history")
    if metadata_history is None:
        metadata = _attribute_or_call(table, "metadata")
        metadata_log = _mapping_or_attribute(metadata, "metadata_log") or ()
        if metadata_log:
            return _metadata_entries_from_metadata_log(metadata, metadata_log)
        return _schema_entries_from_current_metadata(metadata)
    return tuple(metadata_history)


def _retained_metadata_history_complete(table: Any) -> Any:
    return _attribute_or_call(table, "retained_metadata_history_complete")


def _has_uninspected_metadata_log(table: Any) -> bool:
    if _attribute_or_call(table, "metadata_history") is not None:
        return False

    metadata = _attribute_or_call(table, "metadata")
    metadata_log = _mapping_or_attribute(metadata, "metadata_log")
    return bool(metadata_log)


def _schema_entries_from_current_metadata(metadata: Any) -> tuple[Any, ...]:
    if metadata is None:
        return ()
    schemas = _mapping_or_attribute(metadata, "schemas") or ()
    return tuple(
        {
            "schema": schema,
            "properties": {},
            "metadata_file": _metadata_entry_source(metadata),
        }
        for schema in schemas
    )


def _metadata_entries_from_metadata_log(
    current_metadata: Any, metadata_log: Iterable[Any]
) -> tuple[Any, ...]:
    retained_entries = []
    for log_entry in metadata_log:
        metadata_file = _metadata_entry_source(log_entry)
        retained_metadata = _load_retained_table_metadata(metadata_file)
        if retained_metadata is not None:
            retained_entries.append(
                _metadata_entry_from_table_metadata(retained_metadata, metadata_file)
            )

    retained_entries.append(
        _metadata_entry_from_table_metadata(
            current_metadata, _metadata_entry_source(current_metadata)
        )
    )
    return tuple(retained_entries)


def _load_retained_table_metadata(metadata_file: str) -> Any:
    try:
        return _load_static_table(metadata_file).metadata
    except Exception:
        return None


def _metadata_entry_from_table_metadata(metadata: Any, metadata_file: str) -> Any:
    return {
        "schema": _current_schema(metadata),
        "properties": _mapping_or_attribute(metadata, "properties") or {},
        "metadata_file": metadata_file,
    }


def _current_schema(metadata: Any) -> Any:
    current_schema = _mapping_or_attribute(metadata, "current_schema")
    if current_schema is not None:
        return current_schema

    schemas = tuple(_mapping_or_attribute(metadata, "schemas") or ())
    current_schema_id = _mapping_or_attribute(metadata, "current_schema_id")
    if current_schema_id is not None:
        for schema in schemas:
            if _schema_id(schema) == current_schema_id:
                return schema
    if schemas:
        return schemas[-1]
    return None


def _schema_id(schema: Any) -> Any:
    schema_id = _mapping_or_attribute(schema, "schema_id")
    if schema_id is None:
        schema_id = _mapping_or_attribute(schema, "schema-id")
    if schema_id is None:
        schema_id = _mapping_or_attribute(schema, "id")
    return schema_id


def _schema_fields(metadata_entry: Any) -> dict[Any, dict[str, str]]:
    schema = _mapping_or_attribute(metadata_entry, "schema")
    if schema is None:
        return {}

    fields = _mapping_or_attribute(schema, "fields") or ()
    return {
        _field_identity(field): {
            "name": str(_mapping_or_attribute(field, "name")),
            "type": str(_field_type(field)),
            "doc": _field_doc(field),
        }
        for field in fields
    }


def _schema_changes(previous: Any, current: Any) -> tuple[EvolutionChange, ...]:
    previous_fields = _schema_fields(previous)
    current_fields = _schema_fields(current)
    source = _metadata_entry_source(current)
    changes = []

    for field_id in current_fields.keys() - previous_fields.keys():
        field = current_fields[field_id]
        changes.append(
            EvolutionChange(
                change_type="added",
                subject="schema.column",
                name=field["name"],
                before=None,
                after=field["type"],
                source=source,
            )
        )

    for field_id in previous_fields.keys() - current_fields.keys():
        field = previous_fields[field_id]
        changes.append(
            EvolutionChange(
                change_type="removed",
                subject="schema.column",
                name=field["name"],
                before=field["type"],
                after=None,
                source=source,
            )
        )

    for field_id in previous_fields.keys() & current_fields.keys():
        previous_field = previous_fields[field_id]
        current_field = current_fields[field_id]
        if previous_field["name"] != current_field["name"]:
            changes.append(
                EvolutionChange(
                    change_type="renamed",
                    subject="schema.column",
                    name=current_field["name"],
                    before=previous_field["name"],
                    after=current_field["name"],
                    source=source,
                )
            )
        if previous_field["type"] != current_field["type"]:
            changes.append(
                EvolutionChange(
                    change_type="type_changed",
                    subject="schema.column",
                    name=current_field["name"],
                    before=previous_field["type"],
                    after=current_field["type"],
                    source=source,
                )
            )
        if previous_field["doc"] != current_field["doc"]:
            changes.append(
                EvolutionChange(
                    change_type="documentation_changed",
                    subject="schema.column",
                    name=current_field["name"],
                    before=previous_field["doc"],
                    after=current_field["doc"],
                    source=source,
                )
            )

    return tuple(
        sorted(
            changes,
            key=lambda change: (
                {
                    "added": 0,
                    "removed": 1,
                    "renamed": 2,
                    "type_changed": 3,
                    "documentation_changed": 4,
                }[change.change_type],
                change.name,
            ),
        )
    )


def _field_identity(field: Any) -> Any:
    field_id = _mapping_or_attribute(field, "id")
    if field_id is None:
        field_id = _mapping_or_attribute(field, "field_id")
    if field_id is not None:
        return field_id
    return _mapping_or_attribute(field, "name")


def _field_type(field: Any) -> Any:
    field_type = _mapping_or_attribute(field, "type")
    if field_type is not None:
        return field_type
    return _mapping_or_attribute(field, "field_type")


def _field_doc(field: Any) -> Optional[str]:
    doc = _mapping_or_attribute(field, "doc")
    if doc is None:
        return None
    return str(doc)


def _metadata_entry_source(metadata_entry: Any) -> str:
    metadata_file = _mapping_or_attribute(metadata_entry, "metadata_file")
    if metadata_file is None:
        metadata_file = _mapping_or_attribute(metadata_entry, "metadata-file")
    if metadata_file is None:
        metadata_file = _mapping_or_attribute(metadata_entry, "location")
    if metadata_file is not None:
        return str(metadata_file)
    return "iceberg.retained_metadata"


def _property_changes(previous: Any, current: Any) -> tuple[EvolutionChange, ...]:
    previous_properties = _properties(previous)
    current_properties = _properties(current)
    source = _metadata_entry_source(current)
    changes = []

    for name in sorted(current_properties.keys() - previous_properties.keys()):
        changes.append(
            EvolutionChange(
                change_type="added",
                subject="table.property",
                name=name,
                before=None,
                after=current_properties[name],
                source=source,
            )
        )

    for name in sorted(previous_properties.keys() - current_properties.keys()):
        changes.append(
            EvolutionChange(
                change_type="removed",
                subject="table.property",
                name=name,
                before=previous_properties[name],
                after=None,
                source=source,
            )
        )

    for name in sorted(previous_properties.keys() & current_properties.keys()):
        previous_value = previous_properties[name]
        current_value = current_properties[name]
        if previous_value != current_value:
            changes.append(
                EvolutionChange(
                    change_type="changed",
                    subject="table.property",
                    name=name,
                    before=previous_value,
                    after=current_value,
                    source=source,
                )
            )

    return tuple(changes)


def _properties(metadata_entry: Any) -> dict[str, str]:
    properties = _mapping_or_attribute(metadata_entry, "properties") or {}
    return {str(key): str(value) for key, value in properties.items()}


def _attribute_or_call(value: Any, name: str) -> Any:
    attribute = getattr(value, name, None)
    if callable(attribute):
        return attribute()
    return attribute


def _mapping_or_attribute(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return _attribute_or_call(value, name)


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

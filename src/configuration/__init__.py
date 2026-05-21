from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class MetadataFileSourceConfiguration:
    location: str
    kind: str = "metadata_file"


@dataclass(frozen=True)
class GlueCatalogTableSourceConfiguration:
    catalog_name: str
    namespace: tuple[str, ...]
    table_name: str
    aws_profile: str | None = None
    region: str | None = None
    kind: str = "glue_catalog_table"


@dataclass(frozen=True)
class AnalysisPolicy:
    snapshot_retention_days: int = 30
    recommendation_thresholds: Mapping[str, int | float] = field(default_factory=dict)
    history_depth: int = 100


@dataclass(frozen=True)
class RuntimePolicy:
    cache_ttl_seconds: int = 900
    timeout_seconds: int = 30
    max_concurrency: int = 4


@dataclass(frozen=True)
class OutputPolicy:
    export_formats: tuple[str, ...] = ()


@dataclass(frozen=True)
class AnalyzerConfiguration:
    table_source: MetadataFileSourceConfiguration | GlueCatalogTableSourceConfiguration
    analysis: AnalysisPolicy = field(default_factory=AnalysisPolicy)
    runtime: RuntimePolicy = field(default_factory=RuntimePolicy)
    output: OutputPolicy = field(default_factory=OutputPolicy)

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
        ui_overrides: Mapping[str, object] | None = None,
    ) -> "AnalyzerConfiguration":
        values = os.environ if environ is None else environ
        overrides = {} if ui_overrides is None else ui_overrides
        return cls(
            table_source=_table_source_value(values, overrides),
            analysis=AnalysisPolicy(
                snapshot_retention_days=_override_int(
                    overrides,
                    "snapshot_retention_days",
                    _int_value(values, "LHA_SNAPSHOT_RETENTION_DAYS", 30),
                ),
                recommendation_thresholds=_override_mapping(
                    overrides,
                    "recommendation_thresholds",
                    _thresholds_value(values),
                ),
                history_depth=_override_int(
                    overrides,
                    "history_depth",
                    _int_value(values, "LHA_HISTORY_DEPTH", 100),
                ),
            ),
            runtime=RuntimePolicy(
                cache_ttl_seconds=_override_int(
                    overrides,
                    "cache_ttl_seconds",
                    _int_value(values, "LHA_CACHE_TTL_SECONDS", 900),
                ),
                timeout_seconds=_override_int(
                    overrides,
                    "timeout_seconds",
                    _int_value(values, "LHA_TIMEOUT_SECONDS", 30),
                ),
                max_concurrency=_override_int(
                    overrides,
                    "max_concurrency",
                    _int_value(values, "LHA_MAX_CONCURRENCY", 4),
                ),
            ),
        )


def _table_source_value(
    values: Mapping[str, str], overrides: Mapping[str, object]
) -> MetadataFileSourceConfiguration | GlueCatalogTableSourceConfiguration:
    source_kind = str(
        overrides.get(
            "table_source_kind",
            values.get("LHA_TABLE_SOURCE_KIND", "metadata_file"),
        )
    )
    if source_kind == "glue_catalog_table":
        return GlueCatalogTableSourceConfiguration(
            catalog_name=_required_source_value(
                values, overrides, "glue_catalog_name", "LHA_GLUE_CATALOG_NAME"
            ),
            namespace=_namespace_value(values, overrides),
            table_name=_required_source_value(
                values, overrides, "glue_table_name", "LHA_GLUE_TABLE_NAME"
            ),
            aws_profile=_optional_source_value(
                values, overrides, "aws_profile", "LHA_AWS_PROFILE"
            ),
            region=_optional_source_value(
                values, overrides, "aws_region", "LHA_AWS_REGION"
            ),
        )
    if source_kind != "metadata_file":
        raise ValueError(f"Unsupported table source kind: {source_kind}")
    return MetadataFileSourceConfiguration(
        location=_metadata_location_value(values, overrides)
    )


def _required_source_value(
    values: Mapping[str, str],
    overrides: Mapping[str, object],
    override_key: str,
    environment_key: str,
) -> str:
    raw_override = overrides.get(override_key)
    if raw_override is not None:
        return str(raw_override)
    return values[environment_key]


def _optional_source_value(
    values: Mapping[str, str],
    overrides: Mapping[str, object],
    override_key: str,
    environment_key: str,
) -> str | None:
    raw_override = overrides.get(override_key)
    if raw_override is not None:
        return str(raw_override)
    return values.get(environment_key)


def _namespace_value(
    values: Mapping[str, str], overrides: Mapping[str, object]
) -> tuple[str, ...]:
    raw_override = overrides.get("glue_namespace")
    if raw_override is not None:
        if isinstance(raw_override, str):
            return _namespace_parts(raw_override)
        return tuple(str(part) for part in raw_override)
    return _namespace_parts(values["LHA_GLUE_NAMESPACE"])


def _namespace_parts(raw_value: str) -> tuple[str, ...]:
    return tuple(part for part in raw_value.split(".") if part)


def _int_value(values: Mapping[str, str], key: str, default: int) -> int:
    raw_value = values.get(key)
    if raw_value is None:
        return default
    return int(raw_value)


def _metadata_location_value(
    values: Mapping[str, str], overrides: Mapping[str, object]
) -> str:
    raw_override = overrides.get("metadata_location")
    if raw_override is not None:
        return str(raw_override)
    return values["LHA_METADATA_LOCATION"]


def _thresholds_value(values: Mapping[str, str]) -> Mapping[str, int | float]:
    raw_value = values.get("LHA_RECOMMENDATION_THRESHOLDS")
    if raw_value is None:
        return {}
    thresholds = json.loads(raw_value)
    if not isinstance(thresholds, dict):
        raise ValueError("LHA_RECOMMENDATION_THRESHOLDS must be a JSON object")
    return thresholds


def _override_int(overrides: Mapping[str, object], key: str, fallback: int) -> int:
    raw_value = overrides.get(key)
    if raw_value is None:
        return fallback
    return int(raw_value)


def _override_mapping(
    overrides: Mapping[str, object],
    key: str,
    fallback: Mapping[str, int | float],
) -> Mapping[str, int | float]:
    raw_value = overrides.get(key)
    if raw_value is None:
        return fallback
    if not isinstance(raw_value, Mapping):
        raise ValueError(f"{key} must be a mapping")
    return raw_value

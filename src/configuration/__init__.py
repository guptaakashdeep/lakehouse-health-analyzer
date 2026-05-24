from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional dependency not installed
        tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class MetadataFileSourceConfiguration:
    location: str
    kind: str = "metadata_file"


@dataclass(frozen=True)
class GlueCatalogTableSourceConfiguration:
    catalog_name: str
    namespace: tuple[str, ...] = ()
    table_name: str = "__catalog_overview__"
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
    export_directory: str | None = None


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
        values = _configuration_values(
            os.environ if environ is None else environ,
            load_default_file=environ is None,
        )
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
            output=OutputPolicy(
                export_formats=_output_formats_value(values, overrides),
                export_directory=_override_optional_string(
                    overrides,
                    "export_directory",
                    values.get("LHA_EXPORT_DIRECTORY"),
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
            namespace=_optional_namespace_value(values, overrides),
            table_name=_optional_source_value(
                values, overrides, "glue_table_name", "LHA_GLUE_TABLE_NAME"
            )
            or "__catalog_overview__",
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
    if override_key in overrides:
        raw_override = overrides[override_key]
        if raw_override is None:
            return None
        normalized = str(raw_override).strip()
        return normalized or None
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


def _optional_namespace_value(
    values: Mapping[str, str], overrides: Mapping[str, object]
) -> tuple[str, ...]:
    raw_override = overrides.get("glue_namespace")
    if raw_override is not None:
        if isinstance(raw_override, str):
            return _namespace_parts(raw_override)
        return tuple(str(part) for part in raw_override)
    raw_value = values.get("LHA_GLUE_NAMESPACE")
    if raw_value is None:
        return ()
    return _namespace_parts(raw_value)


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


def _output_formats_value(
    values: Mapping[str, str], overrides: Mapping[str, object]
) -> tuple[str, ...]:
    raw_override = overrides.get("export_formats")
    if raw_override is not None:
        if isinstance(raw_override, str):
            return _split_csv_values(raw_override)
        return tuple(
            str(value).strip().lower() for value in raw_override if str(value).strip()
        )
    return _split_csv_values(values.get("LHA_EXPORT_FORMATS", ""))


def _split_csv_values(raw_value: str) -> tuple[str, ...]:
    return tuple(part.strip().lower() for part in raw_value.split(",") if part.strip())


def _override_optional_string(
    overrides: Mapping[str, object], key: str, fallback: str | None
) -> str | None:
    raw_value = overrides.get(key)
    if raw_value is None:
        return fallback
    return str(raw_value)


def _configuration_values(
    values: Mapping[str, str], *, load_default_file: bool
) -> Mapping[str, str]:
    config_path = values.get("LHA_CONFIG_PATH")
    if config_path is None and load_default_file:
        config_path = str(_default_setup_config_path(values))

    setup_file_values: Mapping[str, str] = {}
    if config_path:
        setup_file_values = _load_setup_file_values(Path(config_path))

    if not setup_file_values:
        return values
    return {**setup_file_values, **values}


def _default_setup_config_path(values: Mapping[str, str]) -> Path:
    xdg_config_home = values.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        config_home = Path(xdg_config_home)
    else:
        home = values.get("HOME")
        config_home = Path(home) / ".config" if home else Path.home() / ".config"
    return config_home / "lakehouse-health-analyzer" / "config.toml"


def _load_setup_file_values(path: Path) -> Mapping[str, str]:
    if tomllib is None or not path.exists():
        return {}
    payload = tomllib.loads(path.read_text())
    if not isinstance(payload, dict):
        return {}

    values: dict[str, str] = {}
    table_source = payload.get("table_source")
    if isinstance(table_source, dict):
        _put_string(values, "LHA_TABLE_SOURCE_KIND", table_source.get("table_source_kind"))
        _put_string(values, "LHA_METADATA_LOCATION", table_source.get("metadata_location"))
        _put_string(values, "LHA_GLUE_CATALOG_NAME", table_source.get("glue_catalog_name"))
        _put_string(values, "LHA_GLUE_NAMESPACE", table_source.get("glue_namespace"))
        _put_string(values, "LHA_GLUE_TABLE_NAME", table_source.get("glue_table_name"))
        _put_string(values, "LHA_AWS_PROFILE", table_source.get("aws_profile"))
        _put_string(values, "LHA_AWS_REGION", table_source.get("aws_region"))

    analysis = payload.get("analysis")
    if isinstance(analysis, dict):
        _put_int(values, "LHA_SNAPSHOT_RETENTION_DAYS", analysis.get("snapshot_retention_days"))
        thresholds = analysis.get("recommendation_thresholds")
        if isinstance(thresholds, dict):
            values["LHA_RECOMMENDATION_THRESHOLDS"] = json.dumps(thresholds)
        _put_int(values, "LHA_HISTORY_DEPTH", analysis.get("history_depth"))

    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        _put_int(values, "LHA_CACHE_TTL_SECONDS", runtime.get("cache_ttl_seconds"))
        _put_int(values, "LHA_TIMEOUT_SECONDS", runtime.get("timeout_seconds"))
        _put_int(values, "LHA_MAX_CONCURRENCY", runtime.get("max_concurrency"))

    output = payload.get("output")
    if isinstance(output, dict):
        formats = output.get("export_formats")
        if isinstance(formats, (list, tuple)):
            normalized = tuple(
                str(value).strip().lower() for value in formats if str(value).strip()
            )
            if normalized:
                values["LHA_EXPORT_FORMATS"] = ",".join(normalized)
        _put_string(values, "LHA_EXPORT_DIRECTORY", output.get("export_directory"))

    return values


def _put_string(values: dict[str, str], key: str, raw_value: object) -> None:
    if not isinstance(raw_value, str):
        return
    normalized = raw_value.strip()
    if not normalized:
        return
    values[key] = normalized


def _put_int(values: dict[str, str], key: str, raw_value: object) -> None:
    if not isinstance(raw_value, int):
        return
    values[key] = str(raw_value)

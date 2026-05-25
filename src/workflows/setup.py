from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional dependency not installed
        tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class AwsProfileOption:
    name: str
    region: str | None


@dataclass(frozen=True)
class SetupSelection:
    aws_profile: str | None
    aws_region: str
    glue_catalog_name: str = "glue"


class SetupPrompter(Protocol):
    def select_profile(
        self, profiles: tuple[AwsProfileOption, ...], default_profile: str | None
    ) -> str | None: ...

    def prompt_region(self, default_region: str | None) -> str: ...

    def prompt_catalog_name(self, default_catalog_name: str) -> str: ...

    def confirm_validation(self, default: bool = True) -> bool: ...


class RichSetupPrompter:
    def __init__(self, *, console: object) -> None:
        self._console = console

    def select_profile(
        self, profiles: tuple[AwsProfileOption, ...], default_profile: str | None
    ) -> str | None:
        from rich.prompt import Prompt

        options = ", ".join(profile.name for profile in profiles)
        self._console.print(f"Named AWS profiles: {options}")

        default_choice = default_profile if default_profile else profiles[0].name
        selected = Prompt.ask(
            "AWS profile (leave blank for default credential chain)",
            default=default_choice,
            show_default=True,
            console=self._console,
        ).strip()
        if selected.lower() in {"", "none", "default-chain"}:
            return None
        return selected

    def prompt_region(self, default_region: str | None) -> str:
        from rich.prompt import Prompt

        if default_region:
            return Prompt.ask(
                "Glue AWS region",
                default=default_region,
                show_default=True,
                console=self._console,
            )
        return Prompt.ask("Glue AWS region", console=self._console)

    def prompt_catalog_name(self, default_catalog_name: str) -> str:
        from rich.prompt import Prompt

        return Prompt.ask(
            "Glue catalog name",
            default=default_catalog_name,
            show_default=True,
            console=self._console,
        )

    def confirm_validation(self, default: bool = True) -> bool:
        from rich.prompt import Confirm

        return Confirm.ask(
            "Run lightweight catalog validation now?",
            default=default,
            console=self._console,
        )


def default_setup_config_path(environ: dict[str, str] | None = None) -> Path:
    values = environ or {}
    config_home = values.get("XDG_CONFIG_HOME")
    if not config_home:
        config_home = str(Path.home() / ".config")
    return Path(config_home) / "lakehouse-health-analyzer" / "config.toml"


def default_aws_config_path(environ: dict[str, str] | None = None) -> Path:
    values = environ or {}
    if values.get("AWS_CONFIG_FILE"):
        return Path(values["AWS_CONFIG_FILE"])
    return Path.home() / ".aws" / "config"


def validate_glue_catalog_selection(selection: SetupSelection) -> str | None:
    from pyiceberg.catalog import load_catalog

    properties: dict[str, str] = {"type": "glue"}
    if selection.aws_profile is not None:
        properties["glue.profile-name"] = selection.aws_profile
    properties["glue.region"] = selection.aws_region

    try:
        catalog = load_catalog(selection.glue_catalog_name, **properties)
        catalog.list_namespaces()
    except Exception as exc:  # pragma: no cover - exercised with monkeypatch in tests
        return str(exc) or exc.__class__.__name__
    return None


def write_setup_configuration(path: Path, selection: SetupSelection) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_setup_configuration(selection))


def run_setup_workflow(
    *,
    config_path: Path,
    aws_config_path: Path,
    prompter: SetupPrompter,
    validate_selection: Callable[[SetupSelection], str | None] | None = None,
    emit_message: Callable[[str], None] | None = None,
) -> SetupSelection:
    notify = emit_message or (lambda _: None)
    existing = _load_setup_selection(config_path)
    profiles = discover_named_aws_profiles(aws_config_path)

    default_profile = existing.aws_profile if existing is not None else None
    selected_profile = (
        prompter.select_profile(profiles, default_profile=default_profile)
        if profiles
        else None
    )
    if not profiles:
        notify(
            "No named AWS profiles were found; setup will use the default AWS credential chain."
        )

    inferred_region = _profile_region(profiles, selected_profile)
    region_default = existing.aws_region if existing is not None else inferred_region
    selected_region = prompter.prompt_region(default_region=region_default).strip()
    if not selected_region:
        raise ValueError("AWS region is required for Glue setup.")

    catalog_default = existing.glue_catalog_name if existing is not None else "glue"
    selected_catalog_name = prompter.prompt_catalog_name(
        default_catalog_name=str(catalog_default)
    ).strip()
    if not selected_catalog_name:
        selected_catalog_name = "glue"

    selection = SetupSelection(
        aws_profile=selected_profile or None,
        aws_region=selected_region,
        glue_catalog_name=selected_catalog_name,
    )
    write_setup_configuration(config_path, selection)

    if prompter.confirm_validation(default=True) and validate_selection is not None:
        validation_error = validate_selection(selection)
        if validation_error:
            notify(f"WARNING: {validation_error}")

    notify(f"Saved configuration to {config_path}")
    return selection


def discover_named_aws_profiles(path: Path) -> tuple[AwsProfileOption, ...]:
    if not path.exists():
        return ()

    parser = configparser.RawConfigParser()
    parser.read(path)

    profiles = []
    for section in parser.sections():
        if not section.startswith("profile "):
            continue
        profile_name = section[len("profile ") :].strip()
        if not profile_name:
            continue
        profiles.append(
            AwsProfileOption(
                name=profile_name,
                region=parser.get(section, "region", fallback=None),
            )
        )
    return tuple(profiles)


def _load_setup_selection(path: Path) -> SetupSelection | None:
    if not path.exists() or tomllib is None:
        return None
    payload = tomllib.loads(path.read_text())
    table_source = payload.get("table_source", {})
    if not isinstance(table_source, dict):
        return None
    region = table_source.get("aws_region")
    if not isinstance(region, str) or not region.strip():
        return None
    profile = table_source.get("aws_profile")
    profile_value = None
    if isinstance(profile, str) and profile.strip():
        profile_value = profile.strip()
    catalog_name = table_source.get("glue_catalog_name")
    if not isinstance(catalog_name, str) or not catalog_name.strip():
        catalog_name = "glue"
    return SetupSelection(
        aws_profile=profile_value,
        aws_region=region.strip(),
        glue_catalog_name=catalog_name.strip(),
    )


def _profile_region(
    profiles: tuple[AwsProfileOption, ...], selected_profile: str | None
) -> str | None:
    for profile in profiles:
        if profile.name == selected_profile:
            return profile.region
    return None


def _render_setup_configuration(selection: SetupSelection) -> str:
    profile_value = (
        f'aws_profile = "{selection.aws_profile}"'
        if selection.aws_profile is not None
        else "aws_profile = \"\""
    )
    return "\n".join(
        (
            "# Lakehouse Health Analyzer configuration",
            "# Built-in defaults are overridden by this file, then environment variables.",
            "",
            "[table_source]",
            "# Source kind used for catalog table workflows.",
            'table_source_kind = "glue_catalog_table"',
            "# Glue catalog name. The default AWS Glue catalog is \"glue\".",
            f'glue_catalog_name = "{selection.glue_catalog_name}"',
            "# Optional named AWS profile. Empty uses default AWS credential chain.",
            profile_value,
            "# Required AWS region for Glue API calls.",
            f'aws_region = "{selection.aws_region}"',
            "",
            "[analysis]",
            "# Retained snapshot age used by expirable snapshot candidate recommendations.",
            "snapshot_retention_days = 30",
            "# Recommendation thresholds as a JSON-like TOML inline table.",
            "recommendation_thresholds = {}",
            "# Number of retained metadata files to inspect for evolution history.",
            "history_depth = 100",
            "",
            "[runtime]",
            "# Operator cache time-to-live in seconds.",
            "cache_ttl_seconds = 900",
            "# Analysis timeout per table in seconds.",
            "timeout_seconds = 30",
            "# Maximum concurrent table analyses.",
            "max_concurrency = 4",
            "",
            "[output]",
            "# Comma-less list of default export formats, for example: [\"json\", \"markdown\"].",
            "export_formats = []",
            "# Default export directory; empty means disabled.",
            "export_directory = \"\"",
            "",
        )
    )

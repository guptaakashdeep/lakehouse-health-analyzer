from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration


def test_configuration_loads_metadata_file_source_from_environment():
    config = AnalyzerConfiguration.from_environment(
        {"LHA_METADATA_LOCATION": "/tmp/orders.metadata.json"}
    )

    assert config.table_source.kind == "metadata_file"
    assert config.table_source.location == "/tmp/orders.metadata.json"


def test_ui_metadata_location_override_wins_over_environment():
    config = AnalyzerConfiguration.from_environment(
        {"LHA_METADATA_LOCATION": "/tmp/from-environment.metadata.json"},
        ui_overrides={"metadata_location": "/tmp/from-ui.metadata.json"},
    )

    assert config.table_source.location == "/tmp/from-ui.metadata.json"


def test_ui_metadata_location_override_can_supply_missing_environment_location():
    config = AnalyzerConfiguration.from_environment(
        {},
        ui_overrides={"metadata_location": "/tmp/from-ui.metadata.json"},
    )

    assert config.table_source.location == "/tmp/from-ui.metadata.json"


def test_configuration_loads_glue_catalog_table_source_from_environment():
    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_TABLE_SOURCE_KIND": "glue_catalog_table",
            "LHA_GLUE_CATALOG_NAME": "analytics",
            "LHA_GLUE_NAMESPACE": "sales.curated",
            "LHA_GLUE_TABLE_NAME": "orders",
            "LHA_AWS_PROFILE": "dev",
            "LHA_AWS_REGION": "us-east-1",
        }
    )

    assert config.table_source == GlueCatalogTableSourceConfiguration(
        catalog_name="analytics",
        namespace=("sales", "curated"),
        table_name="orders",
        aws_profile="dev",
        region="us-east-1",
    )


def test_configuration_loads_analysis_and_runtime_policies_from_environment():
    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_METADATA_LOCATION": "/tmp/orders.metadata.json",
            "LHA_SNAPSHOT_RETENTION_DAYS": "45",
            "LHA_RECOMMENDATION_THRESHOLDS": '{"small_file_count": 25}',
            "LHA_HISTORY_DEPTH": "12",
            "LHA_CACHE_TTL_SECONDS": "120",
            "LHA_TIMEOUT_SECONDS": "7",
            "LHA_MAX_CONCURRENCY": "3",
        }
    )

    assert config.analysis.snapshot_retention_days == 45
    assert config.analysis.recommendation_thresholds == {"small_file_count": 25}
    assert config.analysis.history_depth == 12
    assert config.runtime.cache_ttl_seconds == 120
    assert config.runtime.timeout_seconds == 7
    assert config.runtime.max_concurrency == 3


def test_ui_policy_overrides_win_over_environment():
    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_METADATA_LOCATION": "/tmp/orders.metadata.json",
            "LHA_SNAPSHOT_RETENTION_DAYS": "45",
            "LHA_RECOMMENDATION_THRESHOLDS": '{"small_file_count": 25}',
            "LHA_HISTORY_DEPTH": "12",
            "LHA_CACHE_TTL_SECONDS": "120",
            "LHA_TIMEOUT_SECONDS": "7",
            "LHA_MAX_CONCURRENCY": "3",
        },
        ui_overrides={
            "snapshot_retention_days": 14,
            "recommendation_thresholds": {"small_file_count": 10},
            "history_depth": 5,
            "cache_ttl_seconds": 60,
            "timeout_seconds": 2,
            "max_concurrency": 1,
        },
    )

    assert config.analysis.snapshot_retention_days == 14
    assert config.analysis.recommendation_thresholds == {"small_file_count": 10}
    assert config.analysis.history_depth == 5
    assert config.runtime.cache_ttl_seconds == 60
    assert config.runtime.timeout_seconds == 2
    assert config.runtime.max_concurrency == 1


def test_configuration_loads_output_policy_from_environment_and_ui_overrides():
    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_METADATA_LOCATION": "/tmp/orders.metadata.json",
            "LHA_EXPORT_FORMATS": "json, markdown",
            "LHA_EXPORT_DIRECTORY": "/tmp/from-environment",
        },
        ui_overrides={
            "export_formats": ("markdown",),
            "export_directory": "/tmp/from-ui",
        },
    )

    assert config.output.export_formats == ("markdown",)
    assert config.output.export_directory == "/tmp/from-ui"


def test_configuration_loads_output_policy_from_environment():
    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_METADATA_LOCATION": "/tmp/orders.metadata.json",
            "LHA_EXPORT_FORMATS": "json, markdown",
            "LHA_EXPORT_DIRECTORY": "/tmp/report-exports",
        }
    )

    assert config.output.export_formats == ("json", "markdown")
    assert config.output.export_directory == "/tmp/report-exports"


def test_configuration_uses_setup_file_before_environment_and_ui_overrides(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                "[table_source]",
                'table_source_kind = "metadata_file"',
                'metadata_location = "/tmp/from-config.metadata.json"',
                "",
                "[analysis]",
                "snapshot_retention_days = 14",
                "",
                "[runtime]",
                "timeout_seconds = 11",
                "",
            )
        )
    )

    config = AnalyzerConfiguration.from_environment(
        {
            "LHA_CONFIG_PATH": str(config_path),
            "LHA_METADATA_LOCATION": "/tmp/from-environment.metadata.json",
            "LHA_TIMEOUT_SECONDS": "7",
        },
        ui_overrides={"timeout_seconds": 3},
    )

    assert config.table_source.location == "/tmp/from-environment.metadata.json"
    assert config.analysis.snapshot_retention_days == 14
    assert config.runtime.timeout_seconds == 3


def test_configuration_loads_metadata_location_from_setup_file_when_env_missing(
    tmp_path,
):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                "[table_source]",
                'table_source_kind = "metadata_file"',
                'metadata_location = "/tmp/from-config.metadata.json"',
                "",
            )
        )
    )

    config = AnalyzerConfiguration.from_environment(
        {"LHA_CONFIG_PATH": str(config_path)}
    )

    assert config.table_source.location == "/tmp/from-config.metadata.json"


def test_configuration_loads_glue_setup_file_without_default_namespace_or_table(
    tmp_path,
):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                "[table_source]",
                'table_source_kind = "glue_catalog_table"',
                'glue_catalog_name = "glue"',
                'aws_profile = "dev"',
                'aws_region = "us-east-1"',
                "",
            )
        )
    )

    config = AnalyzerConfiguration.from_environment({"LHA_CONFIG_PATH": str(config_path)})

    assert config.table_source == GlueCatalogTableSourceConfiguration(
        catalog_name="glue",
        namespace=(),
        table_name="__catalog_overview__",
        aws_profile="dev",
        region="us-east-1",
    )

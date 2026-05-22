from pathlib import Path

from workflows.setup import (
    AwsProfileOption,
    SetupPrompter,
    SetupSelection,
    run_setup_workflow,
    discover_named_aws_profiles,
    write_setup_configuration,
)


def test_setup_writes_commented_toml_configuration(tmp_path: Path):
    config_path = tmp_path / "config.toml"

    write_setup_configuration(
        config_path,
        SetupSelection(
            aws_profile="dev",
            aws_region="us-east-1",
            glue_catalog_name="glue",
        ),
    )

    content = config_path.read_text()

    assert "# Lakehouse Health Analyzer configuration" in content
    assert 'table_source_kind = "glue_catalog_table"' in content
    assert 'glue_catalog_name = "glue"' in content
    assert 'aws_profile = "dev"' in content
    assert 'aws_region = "us-east-1"' in content
    assert 'snapshot_retention_days = 30' in content
    assert 'history_depth = 100' in content
    assert 'cache_ttl_seconds = 900' in content
    assert "recommendation_thresholds = {}" in content
    assert "glue_namespace" not in content
    assert "glue_table_name" not in content


def test_setup_discovers_named_aws_profiles_and_regions(tmp_path: Path):
    aws_config = tmp_path / "config"
    aws_config.write_text(
        "\n".join(
            (
                "[default]",
                "region = us-west-2",
                "",
                "[profile dev]",
                "region = us-east-1",
                "",
                "[profile qa]",
                "output = json",
                "",
            )
        )
    )

    profiles = discover_named_aws_profiles(aws_config)

    assert profiles == (
        AwsProfileOption(name="dev", region="us-east-1"),
        AwsProfileOption(name="qa", region=None),
    )


def test_setup_ignores_default_profile_when_no_named_profiles_exist(tmp_path: Path):
    aws_config = tmp_path / "config"
    aws_config.write_text(
        "\n".join(
            (
                "[default]",
                "region = us-west-2",
                "",
            )
        )
    )

    assert discover_named_aws_profiles(aws_config) == ()


def test_setup_rerun_uses_existing_values_as_prompt_defaults(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    aws_config = tmp_path / "config"
    aws_config.write_text(
        "\n".join(
            (
                "[profile qa]",
                "region = us-west-1",
                "",
            )
        )
    )
    write_setup_configuration(
        config_path,
        SetupSelection(
            aws_profile="qa",
            aws_region="us-west-1",
            glue_catalog_name="glue",
        ),
    )
    prompts = FakePrompter(
        selected_profile="qa",
        selected_region="us-west-2",
        selected_catalog_name="analytics",
        run_validation=False,
    )

    selection = run_setup_workflow(
        config_path=config_path,
        aws_config_path=aws_config,
        prompter=prompts,
    )

    assert prompts.profile_default == "qa"
    assert prompts.region_default == "us-west-1"
    assert prompts.catalog_default == "glue"
    assert selection == SetupSelection(
        aws_profile="qa",
        aws_region="us-west-2",
        glue_catalog_name="analytics",
    )
    rewritten = config_path.read_text()
    assert 'glue_catalog_name = "analytics"' in rewritten
    assert 'aws_region = "us-west-2"' in rewritten


def test_setup_without_named_profiles_uses_default_chain_and_warns_on_validation(
    tmp_path: Path,
):
    config_path = tmp_path / "config.toml"
    messages = []
    prompts = FakePrompter(
        selected_profile=None,
        selected_region="eu-central-1",
        selected_catalog_name="glue",
        run_validation=True,
    )

    selection = run_setup_workflow(
        config_path=config_path,
        aws_config_path=tmp_path / "missing-aws-config",
        prompter=prompts,
        validate_selection=lambda configured: "unable to list namespaces",
        emit_message=messages.append,
    )

    assert selection.aws_profile is None
    assert selection.aws_region == "eu-central-1"
    assert any("default AWS credential chain" in message for message in messages)
    assert any("WARNING: unable to list namespaces" in message for message in messages)
    content = config_path.read_text()
    assert 'aws_profile = ""' in content
    assert 'aws_region = "eu-central-1"' in content


def test_setup_uses_selected_profile_region_as_default_when_available(tmp_path: Path):
    aws_config = tmp_path / "config"
    aws_config.write_text(
        "\n".join(
            (
                "[profile dev]",
                "region = us-east-1",
                "",
            )
        )
    )
    prompts = FakePrompter(
        selected_profile="dev",
        selected_region="us-east-1",
        selected_catalog_name="glue",
        run_validation=False,
    )

    run_setup_workflow(
        config_path=tmp_path / "config.toml",
        aws_config_path=aws_config,
        prompter=prompts,
    )

    assert prompts.region_default == "us-east-1"


def test_setup_requires_region_when_no_default_or_inferred_value(tmp_path: Path):
    prompts = FakePrompter(
        selected_profile=None,
        selected_region="",
        selected_catalog_name="glue",
        run_validation=False,
    )

    try:
        run_setup_workflow(
            config_path=tmp_path / "config.toml",
            aws_config_path=tmp_path / "missing-config",
            prompter=prompts,
        )
    except ValueError as exc:
        assert str(exc) == "AWS region is required for Glue setup."
    else:
        raise AssertionError("Expected missing region to fail setup.")


class FakePrompter(SetupPrompter):
    def __init__(
        self,
        *,
        selected_profile: str | None,
        selected_region: str,
        selected_catalog_name: str,
        run_validation: bool,
    ) -> None:
        self.selected_profile = selected_profile
        self.selected_region = selected_region
        self.selected_catalog_name = selected_catalog_name
        self.run_validation = run_validation
        self.profile_default = None
        self.region_default = None
        self.catalog_default = None

    def select_profile(
        self,
        profiles: tuple[AwsProfileOption, ...],
        default_profile: str | None,
    ) -> str | None:
        self.profile_default = default_profile
        return self.selected_profile

    def prompt_region(self, default_region: str | None) -> str:
        self.region_default = default_region
        return self.selected_region

    def prompt_catalog_name(self, default_catalog_name: str) -> str:
        self.catalog_default = default_catalog_name
        return self.selected_catalog_name

    def confirm_validation(self, default: bool = True) -> bool:
        return self.run_validation

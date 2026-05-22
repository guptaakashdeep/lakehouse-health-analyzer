from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    EvolutionChange,
    HealthMetric,
    MaintenanceRecommendation,
    PartitionHealthMetric,
    TableHealthReport,
    TableEvolutionHistory,
    TableSource,
)
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from visualization.table_health_report import (
    display_table_detail_view,
    display_table_health_report,
)
from workflows.table_detail import TableDetailSection, TableDetailView, table_detail_view


class FakeStreamlit:
    def __init__(self):
        self.headers = []
        self.subheaders = []
        self.metrics = []
        self.warnings = []
        self.infos = []
        self.errors = []
        self.captions = []
        self.messages = []

    def header(self, text):
        self.headers.append(text)

    def subheader(self, text):
        self.subheaders.append(text)

    def metric(self, label, value):
        self.metrics.append((label, value))

    def warning(self, text):
        self.warnings.append(text)

    def info(self, text):
        self.infos.append(text)

    def error(self, text):
        self.errors.append(text)

    def caption(self, text):
        self.captions.append(text)

    def write(self, text):
        self.messages.append(text)


class FakePartitionStreamlit(FakeStreamlit):
    def __init__(self):
        super().__init__()
        self.dataframes = []

    def dataframe(self, value, **kwargs):
        self.dataframes.append(value)


def test_streamlit_renderer_displays_prepared_table_detail_view(monkeypatch):
    fake_st = FakePartitionStreamlit()
    monkeypatch.setattr("visualization.table_health_report.st", fake_st)

    view = table_detail_view(_detailed_report())

    display_table_detail_view(view)

    assert fake_st.headers == ["Table: sales.orders"]
    assert fake_st.subheaders[:4] == [
        "Recommendations",
        "Warnings",
        "Files",
        "Records",
    ]
    assert fake_st.errors == [
        "Compaction (critical): Compact clustered small files.",
    ]
    assert fake_st.warnings == [
        "partition_size_skewness: Partition sizes were incomplete.",
    ]
    assert ("Data File Count", "7 files") in fake_st.metrics
    assert ("Total File Size", "12 MiB") in fake_st.metrics
    assert fake_st.captions == [
        "Evidence: high_file_count_partition_count=2",
        "Thresholds: high_file_count_partition_count_critical=2",
    ]


def test_streamlit_report_renderer_delegates_to_shared_table_detail_view_builder(
    monkeypatch,
):
    fake_st = FakeStreamlit()
    monkeypatch.setattr("visualization.table_health_report.st", fake_st)

    builder_calls = []
    prepared = TableDetailView(
        table_name="warehouse.sales.orders",
        sections=(
            TableDetailSection(
                key="records",
                title="Records",
                items=(),
            ),
        ),
    )

    def fake_builder(report):
        builder_calls.append(report.table_name)
        return prepared

    monkeypatch.setattr(
        "visualization.table_health_report.table_detail_view", fake_builder
    )

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(),
        display_statistics=(),
    )

    display_table_health_report(report)

    assert builder_calls == ["warehouse.sales.orders"]
    assert fake_st.headers == ["Table: warehouse.sales.orders"]
    assert fake_st.subheaders == ["Records"]


def test_streamlit_report_renderer_displays_metadata_and_partition_rows(monkeypatch):
    fake_st = FakePartitionStreamlit()
    monkeypatch.setattr("visualization.table_health_report.st", fake_st)

    report = TableHealthReport(
        table_name="sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(
            HealthMetric(
                key="partition_count",
                label="Partition Count",
                value=2,
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="high_file_count_partition_count",
                label="High File Count Partition Count",
                value=1,
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
        ),
        display_statistics=(),
        partition_metrics=(
            PartitionHealthMetric(
                partition={"region": "east"},
                data_file_count=4,
                delete_file_count=1,
                total_data_file_size_bytes=400,
                average_data_file_size_bytes=100,
                source="iceberg.inspect.partitions",
            ),
        ),
        table_evolution_history=TableEvolutionHistory(
            schema_changes=(
                EvolutionChange(
                    change_type="added",
                    subject="schema.column",
                    name="customer_id",
                    before=None,
                    after="long",
                    source="/tmp/v2.metadata.json",
                ),
            ),
            property_changes=(
                EvolutionChange(
                    change_type="changed",
                    subject="table.property",
                    name="write.format.default",
                    before="parquet",
                    after="orc",
                    source="/tmp/v2.metadata.json",
                ),
            ),
        ),
    )

    display_table_health_report(report)

    assert "Metadata and Evolution" in fake_st.subheaders
    assert "Partitions" in fake_st.subheaders
    assert ("Partition Count", "2 partitions") in fake_st.metrics
    assert ("High File Count Partition Count", "1 partitions") in fake_st.metrics
    assert len(fake_st.dataframes) == 2
    assert fake_st.dataframes[0].to_dict("records") == [
        {
            "region": "east",
            "data_file_count": 4,
            "delete_file_count": 1,
            "total_data_file_size_bytes": 400,
            "average_data_file_size_bytes": 100,
        }
    ]
    assert fake_st.dataframes[1].to_dict("records") == [
        {
            "change_type": "added",
            "subject": "schema.column",
            "name": "customer_id",
            "before": None,
            "after": "long",
            "source": "/tmp/v2.metadata.json",
        },
        {
            "change_type": "changed",
            "subject": "table.property",
            "name": "write.format.default",
            "before": "parquet",
            "after": "orc",
            "source": "/tmp/v2.metadata.json",
        },
    ]


def test_streamlit_metadata_file_mode_uses_centralized_configuration(monkeypatch):
    import streamlit_app

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(),
        display_statistics=(),
    )

    calls = []

    def fake_analyze(config):
        calls.append(config)
        return report

    monkeypatch.setattr(streamlit_app, "analyze_iceberg_metadata_file", fake_analyze)

    result = streamlit_app.get_table_metrics(
        use_metadata_file=True,
        metadata_location="/tmp/orders.metadata.json",
    )

    assert result is report
    assert len(calls) == 1
    assert isinstance(calls[0], AnalyzerConfiguration)
    assert calls[0].table_source.location == "/tmp/orders.metadata.json"


def test_streamlit_catalog_mode_uses_canonical_report_path(monkeypatch):
    import streamlit_app

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="glue_catalog_table", location="analytics.sales.orders"
        ),
        health_metrics=(),
        display_statistics=(),
    )

    calls = []

    def fake_analyze(config):
        calls.append(config)
        return report

    monkeypatch.setattr(streamlit_app, "analyze_iceberg_table", fake_analyze)

    result = streamlit_app.get_table_metrics(
        table_name="sales.orders",
        catalog_name="analytics",
        use_metadata_file=False,
    )

    assert result is report
    assert len(calls) == 1
    assert isinstance(calls[0], AnalyzerConfiguration)
    assert calls[0].table_source == GlueCatalogTableSourceConfiguration(
        catalog_name="analytics",
        namespace=("sales",),
        table_name="orders",
    )


def test_streamlit_catalog_listing_uses_configured_catalog_source(monkeypatch):
    import streamlit_app

    calls = []

    class FakeCatalog:
        def list_tables(self, namespace):
            calls.append(("list_tables", namespace))
            return (
                ("sales", "orders"),
                ("sales", "customers"),
            )

    def fake_load_catalog(catalog_name, **properties):
        calls.append(("load_catalog", catalog_name, properties))
        return FakeCatalog()

    monkeypatch.setattr(streamlit_app, "load_catalog", fake_load_catalog)

    table_names = streamlit_app.get_catalog_tables(
        catalog_name="analytics",
        namespace="sales",
        aws_profile="dev",
        aws_region="us-east-1",
    )

    assert table_names == ["sales.orders", "sales.customers"]
    assert calls == [
        (
            "load_catalog",
            "analytics",
            {
                "type": "glue",
                "glue.profile-name": "dev",
                "glue.region": "us-east-1",
            },
        ),
        ("list_tables", "sales"),
    ]


def test_streamlit_catalog_listing_rejects_hierarchical_glue_namespace():
    import streamlit_app

    try:
        streamlit_app.get_catalog_tables(
            catalog_name="analytics",
            namespace="sales.curated",
        )
    except ValueError as exc:
        assert "single namespace component" in str(exc)
    else:
        raise AssertionError("Expected hierarchical Glue namespace to be rejected")


def _detailed_report() -> TableHealthReport:
    return TableHealthReport(
        table_name="sales.orders",
        table_source=TableSource(kind="glue_catalog_table", location="sales.orders"),
        health_metrics=(
            HealthMetric(
                key="data_file_count",
                label="Data File Count",
                value=7,
                unit="files",
                source="test.files",
            ),
            HealthMetric(
                key="data_file_record_count",
                label="Data File Record Count",
                value=1200,
                unit="records",
                source="test.records",
            ),
        ),
        display_statistics=(
            DisplayStatistic(
                key="total_file_size",
                label="Total File Size",
                value="12 MiB",
                derived_from=("total_file_size_bytes",),
            ),
        ),
        maintenance_recommendations=(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity="critical",
                evidence={"high_file_count_partition_count": 2},
                thresholds={"high_file_count_partition_count_critical": 2},
                rationale="Compact clustered small files.",
            ),
        ),
        calculation_warnings=(
            CalculationWarning(
                metric_key="partition_size_skewness",
                message="Partition sizes were incomplete.",
            ),
        ),
        partition_metrics=(
            PartitionHealthMetric(
                partition={"region": "east"},
                data_file_count=4,
                delete_file_count=1,
                total_data_file_size_bytes=400,
                average_data_file_size_bytes=100,
                source="test.partitions",
            ),
        ),
    )

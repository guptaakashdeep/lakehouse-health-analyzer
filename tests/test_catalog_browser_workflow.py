from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from workflows.catalog_browser import (
    CatalogBrowserWorkflow,
    CatalogNamespace,
    CatalogNamespaceListing,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
    configured_catalog_browser_workflow,
)


def test_catalog_browser_workflow_lists_accessible_namespaces():
    workflow = CatalogBrowserWorkflow(
        catalog_access=FakeCatalogAccess(
            namespaces=CatalogNamespaceListing(
                namespaces=(
                    CatalogNamespace(name=("sales",), display_name="sales"),
                    CatalogNamespace(name=("finance",), display_name="finance"),
                )
            )
        )
    )

    listing = workflow.list_namespaces()

    assert [namespace.display_name for namespace in listing.namespaces] == [
        "sales",
        "finance",
    ]


def test_catalog_browser_workflow_lazily_lists_tables_for_selected_namespace():
    access = FakeCatalogAccess(
        namespaces=CatalogNamespaceListing(namespaces=()),
        tables=CatalogTableListing(
            namespace=("sales",),
            rows=(
                CatalogTableRow(
                    namespace=("sales",),
                    name="orders",
                    identifier="sales.orders",
                    table_format=TableFormatClassification.ICEBERG,
                    classification_source="glue_parameters",
                ),
            ),
        ),
    )
    workflow = CatalogBrowserWorkflow(catalog_access=access)

    listing = workflow.list_tables(("sales",))

    assert access.requested_table_namespaces == [("sales",)]
    assert listing.rows[0].identifier == "sales.orders"
    assert listing.rows[0].table_format == TableFormatClassification.ICEBERG
    assert listing.rows[0].classification_source == "glue_parameters"


def test_configured_catalog_browser_workflow_uses_injected_catalog_access():
    access = FakeCatalogAccess(
        namespaces=CatalogNamespaceListing(
            namespaces=(CatalogNamespace(name=("sales",), display_name="sales"),)
        )
    )
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("unused",),
            table_name="unused",
            aws_profile="dev",
            region="us-east-1",
        )
    )

    workflow = configured_catalog_browser_workflow(config, catalog_access=access)

    assert workflow.list_namespaces().namespaces[0].display_name == "sales"


class FakeCatalogAccess:
    def __init__(self, namespaces, tables=None):
        self.namespaces = namespaces
        self.tables = tables
        self.requested_table_namespaces = []

    def list_namespaces(self):
        return self.namespaces

    def list_tables(self, namespace):
        self.requested_table_namespaces.append(namespace)
        return self.tables

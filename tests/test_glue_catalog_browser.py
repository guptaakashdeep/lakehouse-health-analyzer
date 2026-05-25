from catalogs.glue import GlueCatalogBrowserAccess
from workflows.catalog_browser import TableFormatClassification


def test_glue_catalog_browser_lists_namespaces_from_paginated_databases():
    glue = FakeGlueClient(
        database_pages=(
            {
                "DatabaseList": [{"Name": "sales"}],
                "NextToken": "next-page",
            },
            {
                "DatabaseList": [{"Name": "finance"}],
            },
        )
    )
    access = GlueCatalogBrowserAccess(glue_client=glue)

    listing = access.list_namespaces()

    assert [namespace.name for namespace in listing.namespaces] == [
        ("sales",),
        ("finance",),
    ]
    assert [call["operation"] for call in glue.calls] == [
        "get_databases",
        "get_databases",
    ]
    assert glue.calls[1]["NextToken"] == "next-page"


def test_glue_catalog_browser_lists_tables_with_parameter_classification():
    glue = FakeGlueClient(
        table_pages=(
            {
                "TableList": [
                    {
                        "Name": "orders",
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {"table_type": "ICEBERG"},
                    },
                    {
                        "Name": "customers",
                        "TableType": "ICEBERG",
                    },
                ],
                "NextToken": "next-page",
            },
            {
                "TableList": [
                    {
                        "Name": "active_orders",
                        "TableType": "VIRTUAL_VIEW",
                        "Parameters": {"table_type": "VIRTUAL_VIEW"},
                    },
                ],
            },
        )
    )
    access = GlueCatalogBrowserAccess(glue_client=glue)

    listing = access.list_tables(("sales",))

    assert [row.name for row in listing.rows] == [
        "active_orders",
        "customers",
        "orders",
    ]
    assert [
        (row.identifier, row.table_format, row.classification_source)
        for row in listing.rows
    ] == [
        ("sales.active_orders", TableFormatClassification.UNKNOWN, "glue_parameters"),
        ("sales.customers", TableFormatClassification.UNKNOWN, "glue_metadata"),
        ("sales.orders", TableFormatClassification.ICEBERG, "glue_parameters"),
    ]
    assert [call["operation"] for call in glue.calls] == [
        "get_tables",
        "get_tables",
    ]
    assert glue.calls[0]["DatabaseName"] == "sales"
    assert glue.calls[1]["NextToken"] == "next-page"
    assert glue.get_table_calls == []


def test_glue_catalog_browser_keeps_table_rows_from_pages_before_partial_failure():
    glue = FakeGlueClient(
        table_pages=(
            {
                "TableList": [
                    {
                        "Name": "orders",
                        "Parameters": {"table_type": "ICEBERG"},
                    },
                ],
                "NextToken": "next-page",
            },
            RuntimeError("glue throttled"),
        )
    )
    access = GlueCatalogBrowserAccess(glue_client=glue)

    listing = access.list_tables(("sales",))

    assert [row.identifier for row in listing.rows] == ["sales.orders"]
    assert [(failure.scope, failure.message) for failure in listing.failures] == [
        ("sales", "glue throttled")
    ]


class FakeGlueClient:
    def __init__(self, database_pages=(), table_pages=()):
        self.database_pages = list(database_pages)
        self.table_pages = list(table_pages)
        self.calls = []
        self.get_table_calls = []

    def get_databases(self, **kwargs):
        self.calls.append({"operation": "get_databases", **kwargs})
        return self.database_pages.pop(0)

    def get_tables(self, **kwargs):
        self.calls.append({"operation": "get_tables", **kwargs})
        response = self.table_pages.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get_table(self, **kwargs):
        self.get_table_calls.append(kwargs)
        raise AssertionError("initial browsing must not call GetTable per table")

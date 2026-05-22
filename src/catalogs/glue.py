from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from configuration import GlueCatalogTableSourceConfiguration
from workflows.catalog_browser import (
    CatalogBrowserFailure,
    CatalogNamespace,
    CatalogNamespaceListing,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
)


@dataclass(frozen=True)
class GlueCatalogBrowserAccess:
    glue_client: Any

    @classmethod
    def from_table_source(
        cls, source: GlueCatalogTableSourceConfiguration
    ) -> "GlueCatalogBrowserAccess":
        return cls(glue_client=_glue_client(source))

    def list_namespaces(self) -> CatalogNamespaceListing:
        namespaces = []
        failures = []
        next_token = None
        while True:
            request = {}
            if next_token:
                request["NextToken"] = next_token
            try:
                response = self.glue_client.get_databases(**request)
            except Exception as exc:
                failures.append(
                    CatalogBrowserFailure(scope="namespaces", message=_message(exc))
                )
                break
            namespaces.extend(
                CatalogNamespace(
                    name=(database["Name"],),
                    display_name=database["Name"],
                )
                for database in response.get("DatabaseList", ())
            )
            next_token = _next_token(response)
            if not next_token:
                break
        return CatalogNamespaceListing(
            namespaces=tuple(namespaces),
            failures=tuple(failures),
        )

    def list_tables(self, namespace: tuple[str, ...]) -> CatalogTableListing:
        database_name = _database_name(namespace)
        rows = []
        failures = []
        next_token = None
        while True:
            request = {"DatabaseName": database_name}
            if next_token:
                request["NextToken"] = next_token
            try:
                response = self.glue_client.get_tables(**request)
            except Exception as exc:
                failures.append(
                    CatalogBrowserFailure(
                        scope=".".join(namespace),
                        message=_message(exc),
                    )
                )
                break
            rows.extend(
                _table_row(namespace, table_metadata)
                for table_metadata in response.get("TableList", ())
            )
            next_token = _next_token(response)
            if not next_token:
                break
        return CatalogTableListing(
            namespace=namespace,
            rows=tuple(sorted(rows, key=lambda row: row.name)),
            failures=tuple(failures),
        )


def _next_token(response: Mapping[str, Any]) -> str | None:
    token = response.get("NextToken")
    if token is None:
        return None
    return str(token)


def _database_name(namespace: tuple[str, ...]) -> str:
    if len(namespace) != 1:
        return ".".join(namespace)
    return namespace[0]


def _table_row(
    namespace: tuple[str, ...], table_metadata: Mapping[str, Any]
) -> CatalogTableRow:
    table_name = str(table_metadata["Name"])
    classification, source = _classify_table_format(table_metadata)
    return CatalogTableRow(
        namespace=namespace,
        name=table_name,
        identifier=".".join((*namespace, table_name)),
        table_format=classification,
        classification_source=source,
    )


def _classify_table_format(
    table_metadata: Mapping[str, Any],
) -> tuple[TableFormatClassification, str]:
    parameters = table_metadata.get("Parameters")
    if isinstance(parameters, Mapping):
        if parameters.get("table_type") == "ICEBERG":
            return TableFormatClassification.ICEBERG, "glue_parameters"
        return TableFormatClassification.UNKNOWN, "glue_parameters"
    return TableFormatClassification.UNKNOWN, "glue_metadata"


def _message(exc: Exception) -> str:
    return str(exc) or exc.__class__.__name__


def _glue_client(source: GlueCatalogTableSourceConfiguration) -> Any:
    import boto3

    session_kwargs = {}
    if source.aws_profile is not None:
        session_kwargs["profile_name"] = source.aws_profile
    if source.region is not None:
        session_kwargs["region_name"] = source.region
    return boto3.Session(**session_kwargs).client("glue")

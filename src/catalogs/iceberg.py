from typing import Dict, List, Optional, Tuple

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from catalogs.base import BaseCatalog, TableMetadata


class IcebergCatalog(BaseCatalog):
    """Iceberg Catalog implementation using pyiceberg's built-in catalog support."""

    def __init__(self, catalog_name: str, **kwargs):
        """Initialize Iceberg Catalog.

        Args:
            catalog_name: Name of the catalog
            **kwargs: Additional arguments for catalog configuration
        """
        self.catalog = load_catalog(catalog_name, **kwargs)
        self._current_table: Optional[Table] = None
        self._current_table_key: Optional[Tuple[str, str]] = None

    def _load_table(self, database: str, table: str) -> Table:
        """Load table from catalog if not already loaded or if different table requested.
        
        Args:
            database: Name of the database
            table: Name of the table
            
        Returns:
            Loaded Iceberg table
        """
        table_key = (database, table)
        if self._current_table is None or self._current_table_key != table_key:
            self._current_table = self.catalog.load_table(table_key)
            self._current_table_key = table_key
        return self._current_table

    def get_table(self, database: str, table: str) -> TableMetadata:
        """Get table metadata from Iceberg Catalog."""
        try:
            iceberg_table = self._load_table(database, table)
            return iceberg_table
        except Exception as e:
            raise Exception(f"Failed to get table {database}.{table}: {str(e)}") from e

    def list_tables(self, database: str) -> List[str]:
        """List all tables in a database."""
        try:
            return list(self.catalog.list_tables(database))
        except Exception as e:
            raise Exception(f"Failed to list tables in database {database}: {str(e)}") from e

    def list_databases(self) -> List[str]:
        """List all databases in the catalog."""
        try:
            return list(self.catalog.list_namespaces())
        except Exception as e:
            raise Exception(f"Failed to list databases: {str(e)}") from e

    def get_table_location(self, database: str, table: str) -> str:
        """Get the location of a table."""
        try:
            iceberg_table = self._load_table(database, table)
            return iceberg_table.location
        except Exception as e:
            raise Exception(
                f"Failed to get table location for {database}.{table}: {str(e)}"
            ) from e

    def get_table_properties(self, database: str, table: str) -> Dict[str, str]:
        """Get table properties from the catalog."""
        try:
            iceberg_table = self._load_table(database, table)
            return iceberg_table.properties
        except Exception as e:
            raise Exception(
                f"Failed to get table properties for {database}.{table}: {str(e)}"
            ) from e

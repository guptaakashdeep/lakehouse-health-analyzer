from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class TableMetadata(BaseModel):
    """Base model for table metadata."""
    name: str
    database: str
    location: str
    properties: Dict[str, str]
    partition_keys: List[str]
    schema: Dict[str, Any]


class BaseCatalog(ABC):
    """Base class for all catalog implementations."""
    
    @abstractmethod
    def get_table(self, database: str, table: str) -> TableMetadata:
        """Get table metadata from the catalog."""
        pass
    
    @abstractmethod
    def list_tables(self, database: str) -> List[str]:
        """List all tables in a database."""
        pass
    
    @abstractmethod
    def list_databases(self) -> List[str]:
        """List all databases in the catalog."""
        pass
    
    @abstractmethod
    def get_table_location(self, database: str, table: str) -> str:
        """Get the location of a table."""
        pass
    
    @abstractmethod
    def get_table_properties(self, database: str, table: str) -> Dict[str, str]:
        """Get table properties from the catalog."""
        pass 
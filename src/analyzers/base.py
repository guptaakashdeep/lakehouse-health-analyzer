from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class LiveTableMetrics(BaseModel):
    """Metrics related to current table state."""
    total_data_files: int
    total_delete_files: int
    total_files_size: int
    data_file_size: int
    delete_file_size: int
    record_count: int


class SnapshotMetrics(BaseModel):
    """Metrics related to table snapshot changes."""
    added_data_files: int
    deleted_data_files: int
    added_delete_files: int
    removed_delete_files: int
    added_records: int
    deleted_records: int
    changed_partition_count: int
    operation: str


class PartitionMetrics(BaseModel):
    """Metrics related to table partitions."""

    # Partition key-value pairs
    partition: Dict[str, Any]
    # File and size metrics
    data_file_count: int
    delete_file_count: int
    total_data_file_size: int
    avg_file_size_per_partition: float
    # avg_record_count: float
    # max_record_count: int
    # min_record_count: int
    # deviation_record_count: float
    # skew_record_count: float
    # avg_file_count: float
    # max_file_count: int
    # min_file_count: int
    # deviation_file_count: float
    # skew_file_count: float
    ## Per-partition metrics
    # per_partition: Dict[str, Dict[str, int]]


class FileMetrics(BaseModel):
    """Metrics related to table files."""

    file_type: str
    file_count: int
    avg_record_count: float
    max_record_count: int
    min_record_count: int
    avg_file_size: float
    max_file_size: int
    min_file_size: int


class TableHealthMetrics(BaseModel):
    """Base model for table health metrics."""

    # Basic metrics
    total_size_bytes: int
    total_files: int
    total_partitions: int
    average_partition_size_bytes: float
    files_per_partition: Dict[str, int]
    partition_sizes_bytes: Dict[str, int]
    orphan_files: List[str]
    partition_skewness: float
    snapshots_to_expire: List[Dict[str, Any]]


class TableMetadataMetrics(BaseModel):
    """Metrics related to table metrics."""

    # New detailed metrics
    table_name: str
    live_table_metrics: LiveTableMetrics
    snapshot_metrics: SnapshotMetrics
    partition_metrics: List[PartitionMetrics]
    file_metrics: List[FileMetrics]
    historical_snapshots: List[Dict[str, Any]] = []


class BaseAnalyzer(ABC):
    """Base class for all table format analyzers."""

    @abstractmethod
    def get_snapshot_metrics(self, database: str, table: str) -> SnapshotMetrics:
        """Get snapshot metrics."""
        pass

    @abstractmethod
    def get_partition_metrics(self, database: str, table: str) -> Dict[str, Any]:
        """Get detailed partition statistics."""
        pass

    @abstractmethod
    def find_orphan_files(self, database: str, table: str) -> List[str]:
        """Find orphan files in the table."""
        pass

    @abstractmethod
    def analyze_partition_skewness(self, database: str, table: str) -> float:
        """Calculate partition skewness metric."""
        pass

    @abstractmethod
    def get_expirable_snapshots(
        self, database: str, table: str
    ) -> List[Dict[str, Any]]:
        """Get list of snapshots that can be expired."""
        pass

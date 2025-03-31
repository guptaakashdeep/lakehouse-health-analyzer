import time
from typing import Any, Dict, List, Optional

import duckdb
from pyiceberg.table import Table, StaticTable

from analyzers.base import (
    BaseAnalyzer,
    TableMetadataMetrics,
    LiveTableMetrics,
    SnapshotMetrics,
)
from catalogs.iceberg import IcebergCatalog


class IcebergAnalyzer(BaseAnalyzer):
    """Iceberg table analyzer implementation."""

    def __init__(
        self,
        database: str = "default",
        table_name: str = "",
        catalog_name: str = "default",
        metadata_path: Optional[str] = None,
        **catalog_properties: Dict[str, Any],
    ):
        """Initialize Iceberg analyzer.

        Args:
            database: Database name (required for catalog-based loading)
            table_name: Table name (required for catalog-based loading)
            catalog_name: Name of the catalog (used only for catalog-based loading)
            metadata_path: Path to the Iceberg metadata file (for metadata-based loading)
            **catalog_properties: Additional properties for catalog configuration
        """
        self.duckdb = duckdb.connect(":memory:")
        self.table = self._load_table(
            database, table_name, catalog_name, metadata_path, catalog_properties
        )

    def _load_table_from_metadata(self, metadata_path: str) -> Table:
        """Load table directly from metadata file.

        Args:
            metadata_path: Path to the metadata.json file

        Returns:
            Loaded Iceberg table
        """
        table = StaticTable.from_metadata(metadata_location=metadata_path)
        # Store table name for dashboard
        self.table_name = table.name()[-1].split("/")[-3]
        return table

    def _load_table_from_catalog(
        self,
        database: str,
        table: str,
        catalog_name: str,
        catalog_properties: Dict[str, Any],
    ) -> Table:
        """Load table using catalog.

        Args:
            database: Database name
            table: Table name
            catalog_name: Name of the catalog
            catalog_properties: Additional catalog configuration

        Returns:
            Loaded Iceberg table
        """
        catalog = IcebergCatalog(catalog_name=catalog_name, **catalog_properties)
        table = catalog.get_table(database, table)
        # Store table name for dashboard
        self.table_name = f"{database}.{table}"
        return table

    def _load_table(
        self,
        database: str,
        table_name: str,
        catalog_name: str,
        metadata_path: Optional[str],
        catalog_properties: Dict[str, Any],
    ) -> Table:
        """Load an Iceberg table either from metadata file or catalog.

        Args:
            database: Database name
            table_name: Table name
            catalog_name: Name of the catalog
            metadata_path: Path to the metadata file
            catalog_properties: Additional catalog configuration

        Returns:
            Loaded Iceberg table
        """
        if metadata_path:
            return self._load_table_from_metadata(metadata_path)
        else:
            return self._load_table_from_catalog(
                database, table_name, catalog_name, catalog_properties
            )

    def get_table_metrics(self) -> TableMetadataMetrics:
        """Get table metadata metrics."""
        current_snapshot = self.table.current_snapshot()
        snapshot_id = current_snapshot.snapshot_id if current_snapshot else None

        live_table_metrics = self.get_live_table_metrics()
        snapshot_metrics = self.get_snapshot_metrics(snapshot_id)
        partition_metrics = self.get_partition_metrics()
        file_metrics = self.get_file_metrics()
        
        # Get historical snapshots for timeline visualization
        historical_snapshots = self.get_historical_snapshots(limit=20)

        print("get_table_metrics() ->", self.table_name)

        return TableMetadataMetrics(
            table_name=self.table_name,
            live_table_metrics=live_table_metrics,
            snapshot_metrics=snapshot_metrics,
            partition_metrics=partition_metrics,
            file_metrics=file_metrics,
            historical_snapshots=historical_snapshots
        )

    def get_live_table_metrics(self) -> LiveTableMetrics:
        """Get current table state metrics."""
        pa_files = self.table.inspect.files()

        # Query to get data file and delete file counts and sizes
        live_table_sql = """
        SELECT 
            COUNT(*) FILTER (WHERE content = 0) as total_data_files,
            COUNT(*) FILTER (WHERE content = 1) as total_delete_files,
            SUM(file_size_in_bytes) as total_files_size,
            SUM(file_size_in_bytes) FILTER (WHERE content = 0) as data_file_size,
            SUM(file_size_in_bytes) FILTER (WHERE content = 1) as delete_file_size,
            (SUM(record_count) FILTER (WHERE content = 0)) - (SUM(record_count) FILTER (WHERE content = 1)) as record_count
        FROM pa_files
        """

        results = self.duckdb.sql(live_table_sql).to_df().to_dict("records")[0]

        # Convert None values to 0
        for key in results:
            if results[key] is None:
                results[key] = 0

        return LiveTableMetrics(
            total_data_files=results.get("total_data_files", 0),
            total_delete_files=results.get("total_delete_files", 0),
            total_files_size=results.get("total_files_size", 0),
            data_file_size=results.get("data_file_size", 0),
            delete_file_size=results.get("delete_file_size", 0),
            record_count=results.get("record_count", 0),
        )

    def _get_empty_snapshot_metrics(self) -> SnapshotMetrics:
        """Return empty snapshot metrics."""
        return SnapshotMetrics(
            added_data_files=0,
            deleted_data_files=0,
            added_delete_files=0,
            removed_delete_files=0,
            added_records=0,
            deleted_records=0,
            changed_partition_count=0,
            operation="",
        )
    
    def _get_snapshot_metrics_col(self, additional_metrics: List[str] = None) -> str:
        metrics_cols = [
            "added-data-files",
            "deleted-data-files",
            "added-delete-files",
            "removed-delete-files",
            "added-records",
            "deleted-records",
            "changed-partition-count",
        ]

        if additional_metrics:
            metrics_cols.extend(additional_metrics)

        # Create SQL query with COALESCE for null handling
        metrics_col_str = ", ".join(
            (
                f"COALESCE(CAST(summary['{col}'] as INTEGER), 0) as {col.replace('-', '_')}"
            )
            for col in metrics_cols
        )
        return metrics_col_str

    # TODO: Duplicate code same as get_snapshot_metrics
    def get_historical_snapshots(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get historical snapshots with their metrics.
        
        Args:
            limit: Maximum number of snapshots to return, ordered by most recent first
            
        Returns:
            List of dictionaries containing snapshot details and metrics
        """
        # Get all snapshots from the table
        pa_snapshots = self.table.inspect.snapshots()
        
        metrics_col_str = self._get_snapshot_metrics_col(
            additional_metrics=[
                "total-data-files",
                "total-delete-files",
                "total-files-size"
            ]
        )
        
        # Get snapshot details including metrics and timestamp
        snap_sql = f"""
        SELECT
            snapshot_id,
            CAST(committed_at as TIMESTAMP) as timestamp,
            parent_id,
            operation,
            {metrics_col_str}
        FROM pa_snapshots
        ORDER BY committed_at DESC
        LIMIT {limit}
        """
        
        results = self.duckdb.sql(snap_sql).to_df()
        
        if results.empty:
            return []
        
        # Convert to list of dictionaries
        snapshots = results.to_dict('records')
        
        # Format timestamps to ISO format for better display
        for snapshot in snapshots:
            if snapshot.get('timestamp'):
                snapshot['timestamp'] = snapshot['timestamp'].isoformat()
        
        return snapshots

    def get_snapshot_metrics(self, snapshot_id: int) -> SnapshotMetrics:
        """Get snapshot-related metrics for the latest snapshot using DuckDB and PyArrow."""
        if not snapshot_id:
            print("No snapshot id found. Returning empty snapshot metrics.")
            return self._get_empty_snapshot_metrics()

        # Get snapshot metrics from the summary
        pa_snapshots = self.table.inspect.snapshots()

        # Create SQL query with COALESCE for null handling
        metrics_col_str = self._get_snapshot_metrics_col()

        snap_sql = f"""
        SELECT
            operation,
            {metrics_col_str},
        FROM pa_snapshots
        WHERE snapshot_id = {snapshot_id}
        """

        results = self.duckdb.sql(snap_sql).to_df()

        if results.empty:
            return self._get_empty_snapshot_metrics()

        record = results.to_dict("records")[0]

        # Create SnapshotMetrics with values from record
        return SnapshotMetrics(
            **record
        )

    def get_partition_metrics(self) -> List[Dict[str, Any]]:
        """Get partition-related metrics including aggregated and per-partition stats."""

        # Referred directly in duckdb_sql
        pa_partitions = self.table.inspect.partitions()

        # Use DuckDB to analyze partition statistics
        partition_results = (
            self.duckdb.sql(
                """
            SELECT
                partition,
                COUNT(*) as file_count,
                SUM(record_count) as record_count,
                SUM(total_data_file_size_in_bytes) as total_size_bytes
            FROM pa_partitions
            GROUP BY partition
        """
            )
            .to_df()
            .to_dict("records")
        )
        return [
            {
                "partition": partition.get("partition"),
                "partition_file_count": partition.get("file_count"),
                "partition_record_count": partition.get("record_count"),
                "partition_total_size_bytes": partition.get("total_size_bytes"),
            }
            for partition in partition_results
        ]

        # def calculate_skewness(values: List[float]) -> float:
        #     if not values:
        #         return 0.0
        #     mean_val = mean(values)
        #     if mean_val == 0:
        #         return 0.0
        #     try:
        #         std_val = stdev(values)
        #         return std_val / mean_val
        #     except:
        #         return 0.0

        # return {
        #     # Aggregated metrics
        #     "avg_record_count": mean(record_counts) if record_counts else 0,
        #     "max_record_count": max(record_counts) if record_counts else 0,
        #     "min_record_count": min(record_counts) if record_counts else 0,
        #     "deviation_record_count": (
        #         stdev(record_counts) if len(record_counts) > 1 else 0
        #     ),
        #     "skew_record_count": calculate_skewness(record_counts),
        #     "avg_file_count": mean(file_counts) if file_counts else 0,
        #     "max_file_count": max(file_counts) if file_counts else 0,
        #     "min_file_count": min(file_counts) if file_counts else 0,
        #     "deviation_file_count": stdev(file_counts) if len(file_counts) > 1 else 0,
        #     "skew_file_count": calculate_skewness(file_counts),
        #     # Per-partition metrics
        #     "per_partition": per_partition,
        # }

    def get_file_metrics(self) -> Dict[str, Any]:
        """Get file-related metrics including aggregated stats."""
        pa_files = self.table.inspect.files()

        files_sql = """SELECT content,
                count(*) as file_count,
                CAST(AVG(record_count) as INT) as avg_record_count, 
                MAX(record_count) as max_record_count, 
                MIN(record_count) as min_record_count, 
                CAST(AVG(file_size_in_bytes) as INT) as avg_file_size, 
                MAX(file_size_in_bytes) as max_file_size, 
                MIN(file_size_in_bytes) as min_file_size 
                FROM pa_files
                GROUP BY content"""

        # Use DuckDB to analyze file statistics
        files_stats = self.duckdb.sql(files_sql).to_df().to_dict("records")

        return [
            {
                "file_type": (
                    "DATA_FILES" if file.get("content") == 0 else "DELETE_FILES"
                ),
                "file_count": file.get("file_count"),
                "avg_record_count": file.get("avg_record_count"),
                "max_record_count": file.get("max_record_count"),
                "min_record_count": file.get("min_record_count"),
                "avg_file_size": file.get("avg_file_size"),
                "max_file_size": file.get("max_file_size"),
                "min_file_size": file.get("min_file_size"),
            }
            for file in files_stats
        ]

    def get_partition_stats(self, database: str, table: str) -> Dict[str, Any]:
        """Get detailed partition statistics for an Iceberg table."""
        iceberg_table = self.table

        # Use DuckDB to analyze partition statistics
        self.duckdb.execute(
            f"""
            CREATE TABLE iceberg_files AS 
            SELECT 
                partition,
                COUNT(*) as file_count,
                SUM(file_size_in_bytes) as total_size_bytes
            FROM iceberg_scan('{database}.{table}')
            GROUP BY partition
        """
        )

        result = self.duckdb.execute(
            """
            SELECT * FROM iceberg_files
        """
        ).fetchall()

        files_per_partition = {str(r[0]): r[1] for r in result}
        partition_sizes_bytes = {str(r[0]): r[2] for r in result}

        return {
            "files_per_partition": files_per_partition,
            "partition_sizes_bytes": partition_sizes_bytes,
        }

    def find_orphan_files(self, database: str, table: str) -> List[str]:
        """Find orphan files in an Iceberg table."""
        iceberg_table = self.table

        # Get all files in the table location
        all_files = set()
        for f in iceberg_table.scan().plan_files():
            all_files.add(f.file_path)

        # TODO: This needs fixing manifests are not being fetched correctly
        # Get files referenced in metadata
        referenced_files = set()
        for snapshot in iceberg_table.snapshots():
            for manifest in snapshot.manifests():
                for entry in manifest.entries():
                    referenced_files.add(entry.data_file.file_path)

        # Find orphan files
        return list(all_files - referenced_files)

    def analyze_partition_skewness(self, database: str, table: str) -> float:
        """Calculate partition skewness metric for an Iceberg table."""
        partition_stats = self.get_partition_stats(database, table)
        partition_sizes = list(partition_stats["partition_sizes_bytes"].values())

        if not partition_sizes:
            return 0.0

        # Calculate coefficient of variation (standard deviation / mean)
        mean_size = sum(partition_sizes) / len(partition_sizes)
        variance = sum((size - mean_size) ** 2 for size in partition_sizes) / len(
            partition_sizes
        )
        std_dev = variance**0.5

        return std_dev / mean_size if mean_size > 0 else 0.0

    def get_expirable_snapshots(
        self, database: str, table: str
    ) -> List[Dict[str, Any]]:
        """Get list of snapshots that can be expired from an Iceberg table."""
        iceberg_table = self.table

        expirable_snapshots = []
        for snapshot in iceberg_table.snapshots():
            # Check if snapshot is older than retention period
            # This is a simplified example - you would want to add more sophisticated logic
            if snapshot.timestamp_ms < (time.time() * 1000) - (
                30 * 24 * 60 * 60 * 1000
            ):  # 30 days
                expirable_snapshots.append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "timestamp_ms": snapshot.timestamp_ms,
                        "manifest_list": snapshot.manifest_list,
                        "summary": snapshot.summary,
                    }
                )

        return expirable_snapshots

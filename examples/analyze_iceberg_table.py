from typing import Optional

from analyzers.iceberg import IcebergAnalyzer
from catalogs.iceberg import IcebergCatalog
from visualization.metrics_dashboard import show_dashboard


def analyze_table(
    database: str,
    table: str,
    *,
    use_metadata_file: bool = False,
    metadata_path: Optional[str] = None,
    catalog_name: str = "lakehouse",
    # Glue catalog specific parameters
    aws_region: Optional[str] = None,
    warehouse_location: Optional[str] = None,
    # REST catalog specific parameters
    rest_catalog_uri: Optional[str] = None,
    rest_catalog_clients: str = "1",
    rest_catalog_cache_ttl: str = "1s",
) -> None:
    """Analyze an Iceberg table and display health metrics.

    Args:
        database: Name of the database
        table: Name of the table
        use_metadata_file: Whether to use a metadata file instead of catalog
        metadata_path: Path to the metadata file (required if use_metadata_file is True)
        catalog_name: Name of the catalog
        aws_region: AWS region for Glue catalog
        warehouse_location: Location of the warehouse
        rest_catalog_uri: URI for REST catalog
        rest_catalog_clients: Number of clients for REST catalog
        rest_catalog_cache_ttl: Cache TTL for REST catalog
    """
    # Create analyzer based on configuration
    if use_metadata_file:
        if not metadata_path:
            raise ValueError("metadata_path must be provided when using metadata file")
        analyzer = IcebergAnalyzer(metadata_path=metadata_path)
    else:
        # Prepare catalog configuration
        catalog_config = {
            "warehouse": warehouse_location,
        }

        if aws_region:
            # Use Glue catalog configuration
            catalog_config["region"] = aws_region
        elif rest_catalog_uri:
            # Use REST catalog configuration
            catalog_config.update(
                {
                    "uri": rest_catalog_uri,
                    "clients": rest_catalog_clients,
                    "client-pool-cache-ttl": rest_catalog_cache_ttl,
                }
            )
        else:
            raise ValueError(
                "Either aws_region (for Glue) or rest_catalog_uri (for REST) must be provided"
            )

        # Create catalog with appropriate configuration
        # catalog = IcebergCatalog(catalog_name=catalog_name, **catalog_config)

        # Create analyzer with catalog
        # analyzer = IcebergAnalyzer(catalog=catalog)
        analyzer = IcebergAnalyzer(metadata_path=metadata_path)

    # Analyze table
    # metrics = analyzer.analyze_table(database, table)
    metrics = analyzer.get_table_metrics()
    return metrics
    # Show dashboard
    # show_dashboard(metrics)

def get_metrics_for_table(
        table_name: str,
        catalog_name: str = None,
        use_metadata_file: bool = False,
        metadata_location: str = None):
    return analyze_table(
        database="default",
        table=table_name,
        use_metadata_file=use_metadata_file,
        metadata_path=metadata_location)

def main():
    """Example usage of the analyze_table function."""
    # Example 1: Using Glue catalog
    # analyze_table(
    #     database="default",
    #     table="nyc_taxi",
    #     aws_region="us-east-1",
    #     warehouse_location="s3://my-bucket/warehouse",
    # )

    # # Example 2: Using REST catalog
    # analyze_table(
    #     database="default",
    #     table="nyc_taxi",
    #     warehouse_location="/path/to/warehouse",
    #     rest_catalog_uri="http://localhost:8181",
    # )

    # Example 3: Using metadata file
    metadata_path = "/Users/akashdeepgupta/Documents/project-repos/pyspark-playground/warehouse/nyc_tlc/yellow_taxi_trips_mor/metadata/v4.metadata.json"
    # analyze_table(
    #     database="default",
    #     table="nyc_taxi",
    #     use_metadata_file=True,
    #     metadata_path=metadata_path,
    # )
    available_tables = ["nyc_tlc.nyc_yellow_taxi_trips", "nyc_tlc.nyc_green_taxi_trips"]
    # get_table_metrics(table_name, catalog_name, use_metadata_file, metadata_location)
    show_dashboard(
        available_tables=available_tables,
        get_table_metrics=get_metrics_for_table,
        catalog_name="my_catalog",
        use_metadata_file=True,  # Enable metadata file mode
        metadata_location=metadata_path # Optional, can also be entered in UI
    )

# Show the dashboard


if __name__ == "__main__":
    main()

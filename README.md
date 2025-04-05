# Lakehouse Health Analyzer

A tool to analyze the health of Lakehouse tables (Iceberg, Hudi, Delta Lake) by providing insights into table metadata, partition statistics, and overall table health.

## Demo

![Lakehouse Health Analyzer Demo](docs/resources/Lakehouse-analyzer.gif)

## Features

- Table metadata analysis
- Snapshot Metadata metrics
- Partition size statistics
- File count per partition
- Orphan file detection (planned)
- Partition skewness analysis (planned)
- Interactive dashboard using Streamlit

## Supported Table Formats

- Apache Iceberg (primary)
- Apache Hudi (planned)
- Delta Lake (planned)

## Supported Catalogs

- AWS Glue Catalog (planned)
- REST Catalog (planned)
- Static Tables Load via `metadata.json` filepath.

## Installation

```bash
pip install lakehouse-health-analyzer
```

## Development Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

## Usage

```python
# Add src to Python path
import sys
import os
import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from visualization.metrics_dashboard import show_dashboard
from analyzers.iceberg import IcebergAnalyzer

def get_table_metrics(table_name=None, catalog_name=None, 
                     use_metadata_file=False, metadata_location=None):
    """Get metrics for an Iceberg table."""
    try:
        # Initialize analyzer based on mode
        if use_metadata_file:
            analyzer = IcebergAnalyzer(metadata_path=metadata_location)
        else:
            # Not supported at the moment
            analyzer = IcebergAnalyzer(database="default", table_name=table_name, 
                                      catalog_name=catalog_name)
        
        # Get metrics
        return analyzer.get_table_metrics()
    except Exception as e:
        print(f"Error getting table metrics: {str(e)}")
        raise e

# Run the dashboard with a function to get table metrics
show_dashboard(
    get_table_metrics=get_table_metrics,
    available_tables=["example.default.table1", "example.default.table2"],
    catalog_name="default",
    use_metadata_file=True  # Default to metadata file mode
)
```

### Running the Dashboard

The easiest way to run the dashboard is using Streamlit:

```bash
# From the project root
streamlit run streamlit_app.py
```

Alternatively, you can run the dashboard directly:

```bash
python run_dashboard.py
```

## Project Structure

```
lakehouse-health-analyzer/
├── src/
│   ├── catalogs/         # Catalog implementations
│   ├── analyzers/        # Table format specific analyzers
│   ├── visualization/    # Dashboard and visualization components
│   │   └── components/   # UI components for the dashboard
│   └── utils/            # Utility functions
├── tests/                # Test files
├── docs/                 # Documentation
├── examples/             # Example notebooks and scripts
├── streamlit_app.py      # Streamlit entry point
└── run_dashboard.py      # Python entry point
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
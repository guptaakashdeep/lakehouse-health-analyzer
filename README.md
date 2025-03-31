# Lakehouse Health Analyzer

A tool to analyze the health of Lakehouse tables (Iceberg, Hudi, Delta Lake) by providing insights into table metadata, partition statistics, and overall table health.

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
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from analyzers.iceberg import IcebergAnalyzer
from visualization.metrics_dashboard import show_dashboard

# Analyze using a metadata file
analyzer = IcebergAnalyzer(metadata_path="/path/to/metadata.json")
metrics = analyzer.get_table_metrics()

# Analyze using a catalog
analyzer = IcebergAnalyzer(database="default", table_name="mytable", catalog_name="glue")
metrics = analyzer.get_table_metrics()
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
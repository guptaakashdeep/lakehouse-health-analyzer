from __future__ import annotations


class UnsupportedTableError(RuntimeError):
    """Raised when a selected table is confirmed to be non-Iceberg."""

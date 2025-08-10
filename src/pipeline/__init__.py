"""Pipeline package for SQL Server incremental data extraction."""

from .core import IncrementalSQLServerParquetPipeline

__all__ = ["IncrementalSQLServerParquetPipeline"]
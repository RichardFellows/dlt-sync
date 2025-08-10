"""
SQL Server Incremental Data Pipeline using DLT

A comprehensive solution for incremental data extraction from SQL Server
with support for partitioned tables, SCD Type 2, and static reference tables.
"""

__version__ = "1.0.0"
__author__ = "DLT Sync Pipeline"

from .pipeline.core import IncrementalSQLServerParquetPipeline

__all__ = ["IncrementalSQLServerParquetPipeline"]
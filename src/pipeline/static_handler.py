"""Static reference table handler with hash-based change detection."""

from typing import Dict, List, Any, Optional, Union, Generator
from datetime import datetime, timedelta
import time
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from sqlalchemy import text
import structlog

from ..config.models import StaticTableConfig, ProcessingConfig
from .sql_utils import SQLServerUtils
from .decimal_handler import DecimalHandler
from .state_manager import StateManager, PipelineState
from ..utils.logging import PipelineLogger

logger = structlog.get_logger(__name__)


class StaticHandler:
    """Handles static reference tables with hash-based change detection."""
    
    def __init__(
        self,
        sql_utils: SQLServerUtils,
        decimal_handler: DecimalHandler,
        state_manager: StateManager,
        processing_config: ProcessingConfig,
        pipeline_logger: PipelineLogger
    ):
        """
        Initialize static handler.
        
        Args:
            sql_utils: SQL Server utilities
            decimal_handler: Decimal precision handler
            state_manager: State manager for tracking progress
            processing_config: Processing configuration
            pipeline_logger: Context-aware logger
        """
        self.sql_utils = sql_utils
        self.decimal_handler = decimal_handler
        self.state_manager = state_manager
        self.config = processing_config
        self.logger = pipeline_logger.bind(handler="static")
    
    def process_table(
        self,
        table_config: StaticTableConfig
    ) -> Dict[str, Any]:
        """
        Process a static reference table with hash-based change detection.
        
        Args:
            table_config: Static table configuration
            
        Returns:
            Processing results dictionary
        """
        start_time = time.time()
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        self.logger.log_table_processing_start(
            table_type="static",
            table_name=table_name,
            hash_algorithm=table_config.hash_algorithm,
            load_strategy=table_config.load_strategy
        )
        
        try:
            # Get or recover state
            current_state = self.state_manager.detect_and_recover_static_state(table_config)
            
            # Compute current table hash
            current_hash = self.sql_utils.get_table_hash(
                table_config.schema_name,
                table_config.table_name,
                table_config.hash_algorithm
            )
            
            # Check if processing is needed
            needs_processing = self._needs_processing(
                current_state, current_hash, table_config
            )
            
            if not needs_processing["should_process"]:
                self.logger.info(
                    "No processing needed for static table",
                    reason=needs_processing["reason"]
                )
                return {
                    "total_records": 0,
                    "processing_needed": False,
                    "reason": needs_processing["reason"],
                    "current_hash": current_hash[:16] + "..." if current_hash else None,
                    "previous_hash": current_state.last_hash[:16] + "..." if current_state.last_hash else None
                }
            
            # Get row count before processing
            total_rows = self.sql_utils.get_table_row_count(
                table_config.schema_name,
                table_config.table_name,
                table_config.custom_where_clause
            )
            
            if total_rows == 0:
                self.logger.warning("Static table is empty")
                return {
                    "total_records": 0,
                    "processing_needed": False,
                    "reason": "table_empty"
                }
            
            # Process based on load strategy
            if table_config.load_strategy == "replace":
                results = self._process_with_replace_strategy(table_config, current_hash, total_rows)
            else:  # upsert
                results = self._process_with_upsert_strategy(table_config, current_hash, total_rows)
            
            # Update state
            new_state = PipelineState(
                table_name=table_name,
                table_type="static",
                last_hash=current_hash,
                records_processed=results["total_records"],
                metadata={
                    "load_strategy": table_config.load_strategy,
                    "hash_algorithm": table_config.hash_algorithm,
                    "duration_seconds": time.time() - start_time,
                    "processing_reason": needs_processing["reason"],
                    "previous_hash": current_state.last_hash if current_state else None
                }
            )
            
            self.state_manager.update_state(new_state)
            
            duration = time.time() - start_time
            self.logger.log_table_processing_end(
                table_type="static",
                table_name=table_name,
                records_processed=results["total_records"],
                duration_seconds=duration,
                load_strategy=table_config.load_strategy,
                hash_changed=current_hash != (current_state.last_hash if current_state else None)
            )
            
            return results
            
        except Exception as e:
            self.logger.error(
                "Static table processing failed",
                table_name=table_name,
                error=str(e),
                duration_seconds=time.time() - start_time
            )
            raise
    
    def _needs_processing(
        self,
        current_state: PipelineState,
        current_hash: str,
        table_config: StaticTableConfig
    ) -> Dict[str, Any]:
        """Determine if static table needs processing."""
        
        if current_state is None:
            return {
                "should_process": True,
                "reason": "initial_load"
            }
        
        # Check if hash has changed
        if current_hash != current_state.last_hash:
            return {
                "should_process": True,
                "reason": "hash_changed"
            }
        
        # Check if forced reload is needed based on age
        if (table_config.force_reload_after_days and current_state.last_updated and
            datetime.utcnow() - current_state.last_updated > timedelta(days=table_config.force_reload_after_days)):
            return {
                "should_process": True,
                "reason": "forced_reload_by_age"
            }
        
        return {
            "should_process": False,
            "reason": "no_changes_detected"
        }
    
    def _process_with_replace_strategy(
        self,
        table_config: StaticTableConfig,
        current_hash: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """Process static table with replace strategy."""
        
        self.logger.info(
            "Processing with replace strategy",
            table_name=f"{table_config.schema_name}.{table_config.table_name}",
            total_rows=total_rows
        )
        
        # Create DLT source with replace disposition
        source = self._create_static_source(table_config, "replace")
        
        pipeline = dlt.pipeline(
            pipeline_name=f"static_{table_config.table_name}",
            destination="parquet",
            dataset_name=table_config.target_schema
        )
        
        # Load with replace disposition
        load_info = pipeline.run(source, write_disposition="replace")
        
        # Count loaded records
        total_loaded = 0
        for package in load_info.load_packages:
            for job in package.jobs:
                if job.job_file_info.table_name == table_config.target_table:
                    total_loaded += job.job_file_info.rows_count or 0
        
        return {
            "total_records": total_loaded,
            "processing_needed": True,
            "load_strategy": "replace",
            "current_hash": current_hash,
            "records_loaded": total_loaded
        }
    
    def _process_with_upsert_strategy(
        self,
        table_config: StaticTableConfig,
        current_hash: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """Process static table with upsert strategy."""
        
        if not table_config.primary_key_columns:
            raise ValueError("Primary key columns required for upsert strategy")
        
        self.logger.info(
            "Processing with upsert strategy",
            table_name=f"{table_config.schema_name}.{table_config.table_name}",
            total_rows=total_rows,
            primary_keys=table_config.primary_key_columns
        )
        
        # Create DLT source with merge disposition
        source = self._create_static_source(table_config, "merge")
        
        pipeline = dlt.pipeline(
            pipeline_name=f"static_{table_config.table_name}",
            destination="parquet",
            dataset_name=table_config.target_schema
        )
        
        # Load with merge disposition
        load_info = pipeline.run(source, write_disposition="merge")
        
        # Count loaded records
        total_loaded = 0
        for package in load_info.load_packages:
            for job in package.jobs:
                if job.job_file_info.table_name == table_config.target_table:
                    total_loaded += job.job_file_info.rows_count or 0
        
        return {
            "total_records": total_loaded,
            "processing_needed": True,
            "load_strategy": "upsert",
            "current_hash": current_hash,
            "records_loaded": total_loaded
        }
    
    def _create_static_source(
        self,
        table_config: StaticTableConfig,
        write_disposition: str
    ) -> dlt.Source:
        """Create DLT source for static table."""
        
        primary_key = table_config.primary_key_columns if write_disposition == "merge" else None
        
        @dlt.resource(
            name=table_config.target_table,
            write_disposition=write_disposition,
            primary_key=primary_key,
            columns=self._get_table_columns_config(table_config)
        )
        def static_data() -> Generator[List[Dict[str, Any]], None, None]:
            batch_size = table_config.batch_size or self.config.batch_size
            
            # Build base query
            query_parts = [f"SELECT * FROM [{table_config.schema_name}].[{table_config.table_name}]"]
            
            # Add WHERE clause if specified
            if table_config.custom_where_clause:
                query_parts.append(f"WHERE {table_config.custom_where_clause}")
            
            # Add ORDER BY for consistent pagination
            if table_config.primary_key_columns:
                order_by_cols = ", ".join([f"[{col}]" for col in table_config.primary_key_columns])
                query_parts.append(f"ORDER BY {order_by_cols}")
            
            base_query = " ".join(query_parts)
            
            # Add pagination
            query = f"""
                {base_query}
                OFFSET :offset ROWS
                FETCH NEXT :batch_size ROWS ONLY
            """
            
            offset = 0
            batch_number = 1
            total_batches = -1  # Unknown total
            
            with self.sql_utils.engine.connect() as conn:
                while True:
                    result = conn.execute(
                        text(query),
                        {"offset": offset, "batch_size": batch_size}
                    ).fetchall()
                    
                    if not result:
                        break
                    
                    batch = [dict(row._mapping) for row in result]
                    
                    # Add hash metadata to each record for tracking
                    for record in batch:
                        record["_dlt_load_id"] = datetime.utcnow().isoformat()
                        record["_dlt_table_hash"] = table_config.hash_algorithm
                    
                    yield batch
                    
                    self.logger.log_batch_processing(
                        batch_number=batch_number,
                        batch_size=len(batch),
                        total_batches=total_batches,
                        table_name=f"{table_config.schema_name}.{table_config.table_name}"
                    )
                    
                    if len(batch) < batch_size:
                        break
                    
                    offset += batch_size
                    batch_number += 1
        
        return dlt.source(static_data)
    
    def _get_table_columns_config(
        self,
        table_config: StaticTableConfig
    ) -> Dict[str, Any]:
        """Get column configuration for DLT resource."""
        try:
            # Get table schema
            schema_info = self.sql_utils.get_table_schema(
                table_config.schema_name,
                table_config.table_name
            )
            
            # Create Arrow schema
            arrow_schema = self.decimal_handler.create_arrow_schema_from_sql_schema(schema_info)
            
            # Convert to DLT column configuration
            columns_config = {}
            for field in arrow_schema:
                column_config = {
                    "data_type": field.type,
                    "nullable": field.nullable
                }
                
                # Add precision information for decimal types
                if pa.types.is_decimal(field.type):
                    column_config.update({
                        "precision": field.type.precision,
                        "scale": field.type.scale
                    })
                
                columns_config[field.name] = column_config
            
            # Add DLT metadata columns
            columns_config["_dlt_load_id"] = {
                "data_type": pa.string(),
                "nullable": True
            }
            columns_config["_dlt_table_hash"] = {
                "data_type": pa.string(),
                "nullable": True
            }
            
            return columns_config
            
        except Exception as e:
            self.logger.warning(
                "Failed to get detailed column configuration",
                table_name=f"{table_config.schema_name}.{table_config.table_name}",
                error=str(e)
            )
            return {}
    
    def analyze_static_table_changes(
        self,
        table_config: StaticTableConfig,
        compare_with_days_ago: int = 7
    ) -> Dict[str, Any]:
        """
        Analyze changes in static table over time.
        
        Args:
            table_config: Static table configuration
            compare_with_days_ago: Compare with hash from N days ago
            
        Returns:
            Analysis results
        """
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        try:
            # Get current hash
            current_hash = self.sql_utils.get_table_hash(
                table_config.schema_name,
                table_config.table_name,
                table_config.hash_algorithm
            )
            
            # Get current state
            current_state = self.state_manager.get_state(table_name)
            
            # Get row count
            current_row_count = self.sql_utils.get_table_row_count(
                table_config.schema_name,
                table_config.table_name,
                table_config.custom_where_clause
            )
            
            analysis = {
                "table_name": table_name,
                "current_hash": current_hash,
                "current_row_count": current_row_count,
                "last_known_hash": current_state.last_hash if current_state else None,
                "last_processed": current_state.last_updated.isoformat() if current_state and current_state.last_updated else None,
                "hash_changed": current_hash != (current_state.last_hash if current_state else None),
                "days_since_last_change": None,
                "change_frequency_estimate": None
            }
            
            if current_state and current_state.last_updated:
                days_since = (datetime.utcnow() - current_state.last_updated).days
                analysis["days_since_last_change"] = days_since
                
                # Simple change frequency estimate
                if days_since > 0:
                    if days_since < 1:
                        analysis["change_frequency_estimate"] = "high"
                    elif days_since < 7:
                        analysis["change_frequency_estimate"] = "medium"
                    else:
                        analysis["change_frequency_estimate"] = "low"
            
            return analysis
            
        except Exception as e:
            self.logger.error(
                "Failed to analyze static table changes",
                table_name=table_name,
                error=str(e)
            )
            return {
                "table_name": table_name,
                "error": str(e),
                "analysis_failed": True
            }
    
    def compare_static_table_hashes(
        self,
        table_config: StaticTableConfig,
        hash_algorithms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compare different hash algorithms for static table.
        
        Args:
            table_config: Static table configuration
            hash_algorithms: List of algorithms to compare (default: all supported)
            
        Returns:
            Hash comparison results
        """
        if hash_algorithms is None:
            hash_algorithms = ["checksum", "hashbytes_md5", "hashbytes_sha256"]
        
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        comparison = {
            "table_name": table_name,
            "algorithms_compared": hash_algorithms,
            "hashes": {},
            "computation_times": {},
            "recommended_algorithm": None
        }
        
        for algorithm in hash_algorithms:
            try:
                start_time = time.time()
                hash_value = self.sql_utils.get_table_hash(
                    table_config.schema_name,
                    table_config.table_name,
                    algorithm
                )
                computation_time = time.time() - start_time
                
                comparison["hashes"][algorithm] = hash_value
                comparison["computation_times"][algorithm] = computation_time
                
                self.logger.debug(
                    "Computed hash with algorithm",
                    algorithm=algorithm,
                    hash_preview=hash_value[:16] + "..." if hash_value else None,
                    computation_time=computation_time
                )
                
            except Exception as e:
                self.logger.warning(
                    "Failed to compute hash with algorithm",
                    algorithm=algorithm,
                    error=str(e)
                )
                comparison["hashes"][algorithm] = None
                comparison["computation_times"][algorithm] = None
        
        # Recommend algorithm based on performance and reliability
        if comparison["computation_times"]:
            fastest_algorithm = min(
                [alg for alg in hash_algorithms if comparison["computation_times"].get(alg)],
                key=lambda x: comparison["computation_times"][x]
            )
            
            # Prefer SHA256 for security, but consider performance
            if "hashbytes_sha256" in comparison["hashes"] and comparison["hashes"]["hashbytes_sha256"]:
                comparison["recommended_algorithm"] = "hashbytes_sha256"
            elif "hashbytes_md5" in comparison["hashes"] and comparison["hashes"]["hashbytes_md5"]:
                comparison["recommended_algorithm"] = "hashbytes_md5"
            elif "checksum" in comparison["hashes"] and comparison["hashes"]["checksum"]:
                comparison["recommended_algorithm"] = "checksum"
            else:
                comparison["recommended_algorithm"] = fastest_algorithm
        
        return comparison
    
    def validate_static_table_integrity(
        self,
        table_config: StaticTableConfig
    ) -> Dict[str, Any]:
        """
        Validate integrity of static table.
        
        Args:
            table_config: Static table configuration
            
        Returns:
            Integrity validation results
        """
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        validation = {
            "table_name": table_name,
            "total_rows": 0,
            "null_counts": {},
            "duplicate_primary_keys": 0,
            "data_quality_issues": [],
            "validation_passed": True
        }
        
        try:
            # Get row count
            validation["total_rows"] = self.sql_utils.get_table_row_count(
                table_config.schema_name,
                table_config.table_name,
                table_config.custom_where_clause
            )
            
            # Check for null values in primary key columns
            if table_config.primary_key_columns:
                for pk_col in table_config.primary_key_columns:
                    null_count_query = text(f"""
                        SELECT COUNT(*) as null_count
                        FROM [{table_config.schema_name}].[{table_config.table_name}]
                        WHERE [{pk_col}] IS NULL
                    """)
                    
                    with self.sql_utils.engine.connect() as conn:
                        result = conn.execute(null_count_query).fetchone()
                        null_count = result.null_count
                        validation["null_counts"][pk_col] = null_count
                        
                        if null_count > 0:
                            validation["data_quality_issues"].append(
                                f"Found {null_count} NULL values in primary key column {pk_col}"
                            )
                            validation["validation_passed"] = False
                
                # Check for duplicate primary keys
                pk_cols = ", ".join([f"[{col}]" for col in table_config.primary_key_columns])
                duplicate_query = text(f"""
                    SELECT COUNT(*) as duplicate_count
                    FROM (
                        SELECT {pk_cols}, COUNT(*) as cnt
                        FROM [{table_config.schema_name}].[{table_config.table_name}]
                        GROUP BY {pk_cols}
                        HAVING COUNT(*) > 1
                    ) duplicates
                """)
                
                with self.sql_utils.engine.connect() as conn:
                    result = conn.execute(duplicate_query).fetchone()
                    validation["duplicate_primary_keys"] = result.duplicate_count
                    
                    if result.duplicate_count > 0:
                        validation["data_quality_issues"].append(
                            f"Found {result.duplicate_count} duplicate primary key combinations"
                        )
                        validation["validation_passed"] = False
            
            # Log validation results
            if validation["validation_passed"]:
                self.logger.info(
                    "Static table integrity validation passed",
                    table_name=table_name,
                    total_rows=validation["total_rows"]
                )
            else:
                self.logger.warning(
                    "Static table integrity validation failed",
                    table_name=table_name,
                    issues=validation["data_quality_issues"]
                )
            
            return validation
            
        except Exception as e:
            self.logger.error(
                "Static table integrity validation failed",
                table_name=table_name,
                error=str(e)
            )
            return {
                "table_name": table_name,
                "error": str(e),
                "validation_failed": True,
                "validation_passed": False
            }
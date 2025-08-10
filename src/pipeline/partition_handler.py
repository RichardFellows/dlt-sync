"""Partition table handler with SQL Server partition switching support."""

from typing import Dict, List, Any, Optional, Union, Generator, Tuple
from datetime import datetime
import concurrent.futures
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import dlt
from dlt.sources.helpers import requests
import structlog

from ..config.models import PartitionTableConfig, ProcessingConfig
from .sql_utils import SQLServerUtils
from .decimal_handler import DecimalHandler
from .state_manager import StateManager, PipelineState
from ..utils.logging import PipelineLogger

logger = structlog.get_logger(__name__)


class PartitionHandler:
    """Handles partition-based incremental tables with SQL Server partition switching."""
    
    def __init__(
        self,
        sql_utils: SQLServerUtils,
        decimal_handler: DecimalHandler,
        state_manager: StateManager,
        processing_config: ProcessingConfig,
        pipeline_logger: PipelineLogger
    ):
        """
        Initialize partition handler.
        
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
        self.logger = pipeline_logger.bind(handler="partition")
        
    def process_table(
        self,
        table_config: PartitionTableConfig
    ) -> Dict[str, Any]:
        """
        Process a partition table with incremental loading.
        
        Args:
            table_config: Partition table configuration
            
        Returns:
            Processing results dictionary
        """
        start_time = time.time()
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        self.logger.log_table_processing_start(
            table_type="partition",
            table_name=table_name,
            partition_column=table_config.partition_column
        )
        
        try:
            # Get or recover state
            current_state = self.state_manager.detect_and_recover_partition_state(table_config)
            
            # Get partition information
            partition_info = self.sql_utils.get_partition_info(
                table_config.schema_name,
                table_config.table_name
            )
            
            # Determine processing strategy
            if (partition_info.get("is_partitioned") and
                self.config.enable_sql_server_partition_switching and
                table_config.enable_partition_switching != False):
                
                results = self._process_with_partition_switching(
                    table_config, current_state, partition_info
                )
            else:
                results = self._process_with_direct_load(
                    table_config, current_state
                )
            
            # Update state
            new_state = PipelineState(
                table_name=table_name,
                table_type="partition",
                last_processed_value=results["last_processed_value"],
                records_processed=results["total_records"],
                metadata={
                    "processing_strategy": results["processing_strategy"],
                    "partitions_processed": results.get("partitions_processed", 0),
                    "duration_seconds": time.time() - start_time
                }
            )
            
            self.state_manager.update_state(new_state)
            
            duration = time.time() - start_time
            self.logger.log_table_processing_end(
                table_type="partition",
                table_name=table_name,
                records_processed=results["total_records"],
                duration_seconds=duration,
                processing_strategy=results["processing_strategy"]
            )
            
            return results
            
        except Exception as e:
            self.logger.error(
                "Partition table processing failed",
                table_name=table_name,
                error=str(e),
                duration_seconds=time.time() - start_time
            )
            raise
    
    def _process_with_partition_switching(
        self,
        table_config: PartitionTableConfig,
        current_state: PipelineState,
        partition_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process using SQL Server partition switching."""
        self.logger.info(
            "Using SQL Server partition switching strategy",
            table_name=f"{table_config.schema_name}.{table_config.table_name}",
            partition_function=partition_info.get("partition_function"),
            total_partitions=len(partition_info.get("partitions", []))
        )
        
        # Get partitions to process
        partitions_to_process = self._get_partitions_to_process(
            table_config, current_state, partition_info
        )
        
        if not partitions_to_process:
            self.logger.info("No new partitions to process")
            return {
                "processing_strategy": "partition_switching",
                "total_records": 0,
                "partitions_processed": 0,
                "last_processed_value": current_state.last_processed_value
            }
        
        # Process partitions with limited parallelism
        total_records = 0
        processed_partitions = []
        last_processed_value = current_state.last_processed_value
        
        # Group partitions for parallel processing
        partition_batches = self._create_partition_batches(
            partitions_to_process, self.config.partition_parallel_limit
        )
        
        for batch in partition_batches:
            batch_results = self._process_partition_batch(table_config, batch)
            
            for result in batch_results:
                if result["success"]:
                    total_records += result["records"]
                    processed_partitions.append(result["partition_info"])
                    last_processed_value = max(
                        last_processed_value or 0,
                        result["partition_info"]["boundary_value"] or 0
                    )
                else:
                    self.logger.error(
                        "Partition processing failed",
                        partition_number=result["partition_info"]["partition_number"],
                        error=result["error"]
                    )
                    # Continue with other partitions but log the failure
        
        return {
            "processing_strategy": "partition_switching",
            "total_records": total_records,
            "partitions_processed": len(processed_partitions),
            "last_processed_value": last_processed_value,
            "processed_partitions": processed_partitions
        }
    
    def _process_with_direct_load(
        self,
        table_config: PartitionTableConfig,
        current_state: PipelineState
    ) -> Dict[str, Any]:
        """Process using direct incremental load."""
        self.logger.info(
            "Using direct incremental load strategy",
            table_name=f"{table_config.schema_name}.{table_config.table_name}"
        )
        
        # Get distinct partition values that need processing
        all_values = self.sql_utils.get_distinct_partition_values(
            table_config.schema_name,
            table_config.table_name,
            table_config.partition_column,
            min_value=current_state.last_processed_value
        )
        
        if not all_values:
            return {
                "processing_strategy": "direct_load",
                "total_records": 0,
                "partitions_processed": 0,
                "last_processed_value": current_state.last_processed_value
            }
        
        # Filter to new values only
        if current_state.last_processed_value is not None:
            new_values = [v for v in all_values if v > current_state.last_processed_value]
        else:
            new_values = all_values
        
        if not new_values:
            self.logger.info("No new partition values to process")
            return {
                "processing_strategy": "direct_load",
                "total_records": 0,
                "partitions_processed": 0,
                "last_processed_value": current_state.last_processed_value
            }
        
        # Process each partition value
        total_records = 0
        batch_size = table_config.batch_size or self.config.batch_size
        
        for partition_value in new_values:
            self.logger.info(
                "Processing partition value",
                partition_value=partition_value,
                progress=f"{new_values.index(partition_value) + 1}/{len(new_values)}"
            )
            
            # Create DLT source for this partition
            source = self._create_partition_source(
                table_config, partition_value, batch_size
            )
            
            # Load data
            pipeline = dlt.pipeline(
                pipeline_name=f"partition_{table_config.table_name}_{partition_value}",
                destination="parquet",
                dataset_name=table_config.target_schema
            )
            
            load_info = pipeline.run(source, write_disposition="append")
            
            # Count records processed
            for package in load_info.load_packages:
                for job in package.jobs:
                    if job.job_file_info.table_name == table_config.target_table:
                        total_records += job.job_file_info.rows_count or 0
            
            self.logger.debug(
                "Partition value processed",
                partition_value=partition_value,
                records=job.job_file_info.rows_count or 0
            )
        
        return {
            "processing_strategy": "direct_load",
            "total_records": total_records,
            "partitions_processed": len(new_values),
            "last_processed_value": max(new_values) if new_values else current_state.last_processed_value
        }
    
    def _get_partitions_to_process(
        self,
        table_config: PartitionTableConfig,
        current_state: PipelineState,
        partition_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get list of partitions that need processing."""
        all_partitions = partition_info.get("partitions", [])
        
        if current_state.last_processed_value is None:
            # First run - process all partitions
            return all_partitions
        
        # Filter partitions based on last processed value
        partitions_to_process = []
        for partition in all_partitions:
            boundary_value = partition.get("boundary_value")
            if boundary_value is None or boundary_value > current_state.last_processed_value:
                partitions_to_process.append(partition)
        
        return partitions_to_process
    
    def _create_partition_batches(
        self,
        partitions: List[Dict[str, Any]],
        max_parallel: int
    ) -> List[List[Dict[str, Any]]]:
        """Create batches of partitions for parallel processing."""
        batches = []
        for i in range(0, len(partitions), max_parallel):
            batches.append(partitions[i:i + max_parallel])
        return batches
    
    def _process_partition_batch(
        self,
        table_config: PartitionTableConfig,
        partition_batch: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process a batch of partitions in parallel."""
        results = []
        
        # Use ThreadPoolExecutor for I/O bound partition switching operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(partition_batch)) as executor:
            future_to_partition = {
                executor.submit(
                    self._process_single_partition_switch,
                    table_config,
                    partition
                ): partition
                for partition in partition_batch
            }
            
            for future in concurrent.futures.as_completed(future_to_partition):
                partition = future_to_partition[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        "success": False,
                        "partition_info": partition,
                        "error": str(e),
                        "records": 0
                    })
        
        return results
    
    def _process_single_partition_switch(
        self,
        table_config: PartitionTableConfig,
        partition_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process a single partition using partition switching."""
        partition_number = partition_info["partition_number"]
        staging_table = f"staging_{table_config.table_name}_{partition_number}"
        
        try:
            # Check prerequisites for partition switching
            prerequisites = self.sql_utils.check_partition_switch_prerequisites(
                table_config.schema_name,
                table_config.table_name,
                table_config.target_schema,
                staging_table,
                partition_number
            )
            
            if not prerequisites["can_switch"]:
                raise Exception(f"Partition switch prerequisites not met: {prerequisites}")
            
            # Create staging table
            staging_created = self.sql_utils.create_staging_table_like(
                table_config.schema_name,
                table_config.table_name,
                table_config.target_schema,
                staging_table
            )
            
            if not staging_created:
                raise Exception("Failed to create staging table")
            
            # Execute partition switch
            switch_success = self.sql_utils.execute_partition_switch(
                table_config.schema_name,
                table_config.table_name,
                table_config.target_schema,
                staging_table,
                partition_number,
                1  # Switch to partition 1 of staging table
            )
            
            if not switch_success:
                raise Exception("Partition switch operation failed")
            
            # Get row count from staging table
            row_count = self.sql_utils.get_table_row_count(
                table_config.target_schema,
                staging_table
            )
            
            self.logger.log_partition_switch(
                source_table=f"{table_config.schema_name}.{table_config.table_name}",
                target_table=f"{table_config.target_schema}.{staging_table}",
                partition_value=partition_info.get("boundary_value"),
                success=True,
                records=row_count
            )
            
            return {
                "success": True,
                "partition_info": partition_info,
                "records": row_count,
                "staging_table": staging_table
            }
            
        except Exception as e:
            self.logger.log_partition_switch(
                source_table=f"{table_config.schema_name}.{table_config.table_name}",
                target_table=f"{table_config.target_schema}.{staging_table}",
                partition_value=partition_info.get("boundary_value"),
                success=False,
                error=str(e)
            )
            
            # Clean up staging table on failure
            try:
                self.sql_utils.cleanup_staging_tables(
                    table_config.target_schema,
                    f"{staging_table}%"
                )
            except Exception:
                pass  # Best effort cleanup
            
            return {
                "success": False,
                "partition_info": partition_info,
                "error": str(e),
                "records": 0
            }
    
    def _create_partition_source(
        self,
        table_config: PartitionTableConfig,
        partition_value: Any,
        batch_size: int
    ) -> dlt.Source:
        """Create DLT source for a specific partition value."""
        
        @dlt.resource(
            name=table_config.target_table,
            write_disposition="append",
            columns=self._get_table_columns_config(table_config)
        )
        def partition_data() -> Generator[List[Dict[str, Any]], None, None]:
            # Build WHERE clause
            where_parts = [f"[{table_config.partition_column}] = :partition_value"]
            
            if table_config.custom_where_clause:
                where_parts.append(f"({table_config.custom_where_clause})")
            
            where_clause = " AND ".join(where_parts)
            
            # Build query
            query = f"""
                SELECT * 
                FROM [{table_config.schema_name}].[{table_config.table_name}]
                WHERE {where_clause}
                ORDER BY [{table_config.partition_column}]
            """
            
            # Execute query in batches
            with self.sql_utils.engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(
                    query, {"partition_value": partition_value}
                )
                
                batch = []
                for row in result:
                    batch.append(dict(row._mapping))
                    
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                
                # Yield remaining records
                if batch:
                    yield batch
        
        return dlt.source(partition_data)
    
    def _get_table_columns_config(
        self,
        table_config: PartitionTableConfig
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
            
            return columns_config
            
        except Exception as e:
            self.logger.warning(
                "Failed to get detailed column configuration",
                table_name=f"{table_config.schema_name}.{table_config.table_name}",
                error=str(e)
            )
            return {}
    
    def cleanup_staging_tables(
        self,
        table_config: PartitionTableConfig,
        pattern: Optional[str] = None
    ) -> int:
        """
        Clean up staging tables for a partition table.
        
        Args:
            table_config: Partition table configuration
            pattern: Optional custom pattern (default: staging_{table_name}_%)
            
        Returns:
            Number of tables cleaned up
        """
        if pattern is None:
            pattern = f"staging_{table_config.table_name}_%"
        
        return self.sql_utils.cleanup_staging_tables(
            table_config.target_schema,
            pattern
        )
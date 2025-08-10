"""SCD Type 2 table handler with LastModified and IsLatest logic."""

from typing import Dict, List, Any, Optional, Union, Generator, Tuple
from datetime import datetime, timedelta
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import dlt
from dlt.sources.helpers import requests
from sqlalchemy import text
import structlog

from ..config.models import SCDTableConfig, ProcessingConfig
from .sql_utils import SQLServerUtils
from .decimal_handler import DecimalHandler
from .state_manager import StateManager, PipelineState
from ..utils.logging import PipelineLogger

logger = structlog.get_logger(__name__)


class SCDHandler:
    """Handles SCD Type 2 tables with LastModified and IsLatest logic."""
    
    def __init__(
        self,
        sql_utils: SQLServerUtils,
        decimal_handler: DecimalHandler,
        state_manager: StateManager,
        processing_config: ProcessingConfig,
        pipeline_logger: PipelineLogger
    ):
        """
        Initialize SCD handler.
        
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
        self.logger = pipeline_logger.bind(handler="scd")
    
    def process_table(
        self,
        table_config: SCDTableConfig
    ) -> Dict[str, Any]:
        """
        Process an SCD Type 2 table with incremental loading.
        
        Args:
            table_config: SCD table configuration
            
        Returns:
            Processing results dictionary
        """
        start_time = time.time()
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        self.logger.log_table_processing_start(
            table_type="scd",
            table_name=table_name,
            business_keys=table_config.business_key_columns,
            last_modified_column=table_config.last_modified_column
        )
        
        try:
            # Get or recover state
            current_state = self.state_manager.detect_and_recover_scd_state(table_config)
            
            # Validate SCD configuration
            scd_validation = self._validate_scd_configuration(table_config)
            if not scd_validation["is_valid"]:
                raise ValueError(f"SCD configuration validation failed: {scd_validation['errors']}")
            
            # Determine the timestamp range to process
            processing_range = self._determine_processing_range(table_config, current_state)
            
            if processing_range["start_timestamp"] is None:
                self.logger.info("No new records to process")
                return {
                    "total_records": 0,
                    "new_records": 0,
                    "updated_records": 0,
                    "last_processed_timestamp": current_state.last_processed_value
                }
            
            # Process records in the determined range
            results = self._process_scd_records(table_config, processing_range)
            
            # Update state
            new_state = PipelineState(
                table_name=table_name,
                table_type="scd",
                last_processed_value=results["last_processed_timestamp"],
                records_processed=results["total_records"],
                metadata={
                    "processing_range": processing_range,
                    "new_records": results["new_records"],
                    "updated_records": results["updated_records"],
                    "duration_seconds": time.time() - start_time,
                    "validation_results": scd_validation
                }
            )
            
            self.state_manager.update_state(new_state)
            
            duration = time.time() - start_time
            self.logger.log_table_processing_end(
                table_type="scd",
                table_name=table_name,
                records_processed=results["total_records"],
                duration_seconds=duration,
                new_records=results["new_records"],
                updated_records=results["updated_records"]
            )
            
            return results
            
        except Exception as e:
            self.logger.error(
                "SCD table processing failed",
                table_name=table_name,
                error=str(e),
                duration_seconds=time.time() - start_time
            )
            raise
    
    def _validate_scd_configuration(
        self,
        table_config: SCDTableConfig
    ) -> Dict[str, Any]:
        """Validate SCD Type 2 configuration."""
        try:
            from ..utils.validation import validate_scd_configuration
            
            validation_result = validate_scd_configuration(
                self.sql_utils.engine,
                table_config.schema_name,
                table_config.table_name,
                table_config.business_key_columns,
                table_config.last_modified_column,
                table_config.is_latest_column
            )
            
            validation_result["is_valid"] = validation_result["validation_passed"]
            validation_result["errors"] = []
            
            if validation_result["integrity_violations_count"] > 0:
                validation_result["errors"].append(
                    f"Found {validation_result['integrity_violations_count']} integrity violations "
                    f"(multiple IsLatest=1 for same business key)"
                )
            
            return validation_result
            
        except Exception as e:
            return {
                "is_valid": False,
                "errors": [f"Validation failed: {str(e)}"],
                "integrity_violations_count": -1
            }
    
    def _determine_processing_range(
        self,
        table_config: SCDTableConfig,
        current_state: PipelineState
    ) -> Dict[str, Any]:
        """Determine the timestamp range to process for SCD updates."""
        
        # Get min/max timestamps from source table
        min_ts, max_ts = self.sql_utils.get_min_max_timestamp(
            table_config.schema_name,
            table_config.table_name,
            table_config.last_modified_column
        )
        
        if max_ts is None:
            return {
                "start_timestamp": None,
                "end_timestamp": None,
                "reason": "no_data_in_source"
            }
        
        # Determine start timestamp
        start_timestamp = current_state.last_processed_value
        
        if start_timestamp is None:
            # First run - start from minimum timestamp
            start_timestamp = min_ts
            reason = "initial_load"
        else:
            # Incremental run - start from last processed timestamp
            if isinstance(start_timestamp, str):
                start_timestamp = datetime.fromisoformat(start_timestamp)
            reason = "incremental_update"
        
        # Check if there are new records to process
        if start_timestamp and start_timestamp >= max_ts:
            return {
                "start_timestamp": None,
                "end_timestamp": None,
                "reason": "no_new_records"
            }
        
        return {
            "start_timestamp": start_timestamp,
            "end_timestamp": max_ts,
            "reason": reason,
            "source_min_timestamp": min_ts,
            "source_max_timestamp": max_ts
        }
    
    def _process_scd_records(
        self,
        table_config: SCDTableConfig,
        processing_range: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process SCD Type 2 records in the given range."""
        
        batch_size = table_config.batch_size or self.config.batch_size
        start_ts = processing_range["start_timestamp"]
        end_ts = processing_range["end_timestamp"]
        
        # Get changed records from source
        changed_records = list(self._get_changed_records(
            table_config, start_ts, end_ts, batch_size
        ))
        
        if not changed_records:
            return {
                "total_records": 0,
                "new_records": 0,
                "updated_records": 0,
                "last_processed_timestamp": processing_range["end_timestamp"]
            }
        
        # Process records for SCD Type 2 logic
        scd_results = self._apply_scd_type2_logic(table_config, changed_records)
        
        # Create DLT source and load data
        source = self._create_scd_source(table_config, scd_results["records_to_load"])
        
        pipeline = dlt.pipeline(
            pipeline_name=f"scd_{table_config.table_name}",
            destination="parquet",
            dataset_name=table_config.target_schema
        )
        
        # Load with merge disposition to handle SCD updates
        load_info = pipeline.run(source, write_disposition="merge")
        
        # Count loaded records
        total_loaded = 0
        for package in load_info.load_packages:
            for job in package.jobs:
                if job.job_file_info.table_name == table_config.target_table:
                    total_loaded += job.job_file_info.rows_count or 0
        
        return {
            "total_records": len(changed_records),
            "new_records": scd_results["new_business_keys"],
            "updated_records": scd_results["updated_business_keys"],
            "records_loaded": total_loaded,
            "last_processed_timestamp": end_ts,
            "business_keys_processed": scd_results["total_business_keys"]
        }
    
    def _get_changed_records(
        self,
        table_config: SCDTableConfig,
        start_timestamp: datetime,
        end_timestamp: datetime,
        batch_size: int
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Get changed records from source table in batches."""
        
        # Build WHERE clause
        where_parts = [
            f"[{table_config.last_modified_column}] > :start_timestamp",
            f"[{table_config.last_modified_column}] <= :end_timestamp"
        ]
        
        if table_config.custom_where_clause:
            where_parts.append(f"({table_config.custom_where_clause})")
        
        where_clause = " AND ".join(where_parts)
        
        # Build query with pagination
        query = f"""
            SELECT *
            FROM [{table_config.schema_name}].[{table_config.table_name}]
            WHERE {where_clause}
            ORDER BY [{table_config.last_modified_column}]
            OFFSET :offset ROWS
            FETCH NEXT :batch_size ROWS ONLY
        """
        
        offset = 0
        
        with self.sql_utils.engine.connect() as conn:
            while True:
                result = conn.execute(
                    text(query),
                    {
                        "start_timestamp": start_timestamp,
                        "end_timestamp": end_timestamp,
                        "offset": offset,
                        "batch_size": batch_size
                    }
                ).fetchall()
                
                if not result:
                    break
                
                batch = [dict(row._mapping) for row in result]
                yield batch
                
                if len(batch) < batch_size:
                    break
                
                offset += batch_size
                
                self.logger.log_batch_processing(
                    batch_number=offset // batch_size,
                    batch_size=len(batch),
                    total_batches=-1  # Unknown total
                )
    
    def _apply_scd_type2_logic(
        self,
        table_config: SCDTableConfig,
        changed_records: List[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Apply SCD Type 2 logic to changed records."""
        
        # Flatten all batches
        all_records = []
        for batch in changed_records:
            all_records.extend(batch)
        
        if not all_records:
            return {
                "records_to_load": [],
                "new_business_keys": 0,
                "updated_business_keys": 0,
                "total_business_keys": 0
            }
        
        # Group records by business key
        business_key_groups = self._group_by_business_key(table_config, all_records)
        
        records_to_load = []
        new_business_keys = 0
        updated_business_keys = 0
        
        for business_key, records in business_key_groups.items():
            # Sort by LastModified to get the latest version
            records.sort(key=lambda r: r[table_config.last_modified_column])
            latest_record = records[-1]
            
            # Get existing record from target (if any)
            existing_record = self._get_existing_scd_record(table_config, business_key)
            
            if existing_record is None:
                # New business key
                latest_record[table_config.is_latest_column] = True
                if table_config.effective_date_column:
                    latest_record[table_config.effective_date_column] = latest_record[table_config.last_modified_column]
                records_to_load.append(latest_record)
                new_business_keys += 1
            else:
                # Existing business key - check if data changed (excluding timestamps)
                if self._has_data_changed(existing_record, latest_record, table_config):
                    # Data changed - create new version
                    # First, mark existing record as not latest
                    existing_record[table_config.is_latest_column] = False
                    if table_config.end_date_column:
                        existing_record[table_config.end_date_column] = latest_record[table_config.last_modified_column]
                    records_to_load.append(existing_record)
                    
                    # Then, add new version as latest
                    latest_record[table_config.is_latest_column] = True
                    if table_config.effective_date_column:
                        latest_record[table_config.effective_date_column] = latest_record[table_config.last_modified_column]
                    records_to_load.append(latest_record)
                    updated_business_keys += 1
        
        self.logger.info(
            "Applied SCD Type 2 logic",
            total_source_records=len(all_records),
            unique_business_keys=len(business_key_groups),
            new_business_keys=new_business_keys,
            updated_business_keys=updated_business_keys,
            records_to_load=len(records_to_load)
        )
        
        return {
            "records_to_load": records_to_load,
            "new_business_keys": new_business_keys,
            "updated_business_keys": updated_business_keys,
            "total_business_keys": len(business_key_groups)
        }
    
    def _group_by_business_key(
        self,
        table_config: SCDTableConfig,
        records: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group records by business key."""
        groups = {}
        
        for record in records:
            # Create business key tuple
            business_key_values = []
            for col in table_config.business_key_columns:
                business_key_values.append(record.get(col))
            
            business_key = tuple(business_key_values)
            
            if business_key not in groups:
                groups[business_key] = []
            
            groups[business_key].append(record)
        
        return groups
    
    def _get_existing_scd_record(
        self,
        table_config: SCDTableConfig,
        business_key: Tuple[Any, ...]
    ) -> Optional[Dict[str, Any]]:
        """Get existing SCD record for business key."""
        try:
            # Build WHERE clause for business key
            where_parts = []
            params = {}
            
            for i, col in enumerate(table_config.business_key_columns):
                param_name = f"bk_{i}"
                where_parts.append(f"[{col}] = :{param_name}")
                params[param_name] = business_key[i]
            
            where_parts.append(f"[{table_config.is_latest_column}] = 1")
            where_clause = " AND ".join(where_parts)
            
            query = f"""
                SELECT *
                FROM [{table_config.target_schema}].[{table_config.target_table}]
                WHERE {where_clause}
            """
            
            with self.sql_utils.engine.connect() as conn:
                result = conn.execute(text(query), params).fetchone()
                
                if result:
                    return dict(result._mapping)
                return None
                
        except Exception as e:
            self.logger.debug(
                "Could not retrieve existing SCD record",
                business_key=business_key,
                error=str(e)
            )
            return None
    
    def _has_data_changed(
        self,
        existing_record: Dict[str, Any],
        new_record: Dict[str, Any],
        table_config: SCDTableConfig
    ) -> bool:
        """Check if data has changed between existing and new records."""
        
        # Columns to exclude from comparison
        exclude_columns = {
            table_config.last_modified_column,
            table_config.is_latest_column
        }
        
        if table_config.effective_date_column:
            exclude_columns.add(table_config.effective_date_column)
        
        if table_config.end_date_column:
            exclude_columns.add(table_config.end_date_column)
        
        # Compare all other columns
        for col_name in new_record.keys():
            if col_name not in exclude_columns:
                existing_value = existing_record.get(col_name)
                new_value = new_record.get(col_name)
                
                # Handle None/null comparisons
                if existing_value is None and new_value is None:
                    continue
                
                if existing_value != new_value:
                    return True
        
        return False
    
    def _create_scd_source(
        self,
        table_config: SCDTableConfig,
        records_to_load: List[Dict[str, Any]]
    ) -> dlt.Source:
        """Create DLT source for SCD records."""
        
        @dlt.resource(
            name=table_config.target_table,
            write_disposition="merge",
            primary_key=table_config.business_key_columns + [table_config.is_latest_column],
            columns=self._get_table_columns_config(table_config)
        )
        def scd_data() -> List[Dict[str, Any]]:
            return records_to_load
        
        return dlt.source(scd_data)
    
    def _get_table_columns_config(
        self,
        table_config: SCDTableConfig
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
    
    def repair_scd_integrity(
        self,
        table_config: SCDTableConfig,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Repair SCD integrity issues (multiple IsLatest=1 for same business key).
        
        Args:
            table_config: SCD table configuration
            dry_run: If True, only report issues without fixing
            
        Returns:
            Repair results
        """
        table_name = f"{table_config.schema_name}.{table_config.table_name}"
        
        self.logger.info(
            "Starting SCD integrity repair",
            table_name=table_name,
            dry_run=dry_run
        )
        
        # Find integrity violations
        business_key_cols = ", ".join(table_config.business_key_columns)
        violations_query = text(f"""
            SELECT 
                {business_key_cols},
                COUNT(*) as violation_count,
                STRING_AGG(CAST([{table_config.last_modified_column}] AS VARCHAR), ', ') as timestamps
            FROM [{table_config.target_schema}].[{table_config.target_table}]
            WHERE [{table_config.is_latest_column}] = 1
            GROUP BY {business_key_cols}
            HAVING COUNT(*) > 1
        """)
        
        with self.sql_utils.engine.connect() as conn:
            violations = conn.execute(violations_query).fetchall()
        
        if not violations:
            return {
                "violations_found": 0,
                "violations_repaired": 0,
                "dry_run": dry_run
            }
        
        violations_repaired = 0
        
        if not dry_run:
            # Repair each violation by keeping only the latest record
            for violation in violations:
                business_key_values = []
                for i, col in enumerate(table_config.business_key_columns):
                    business_key_values.append(getattr(violation, col))
                
                # Get all records for this business key
                where_parts = []
                params = {}
                for i, col in enumerate(table_config.business_key_columns):
                    param_name = f"bk_{i}"
                    where_parts.append(f"[{col}] = :{param_name}")
                    params[param_name] = business_key_values[i]
                
                where_clause = " AND ".join(where_parts)
                
                # Find the latest record
                latest_query = text(f"""
                    SELECT TOP 1 *
                    FROM [{table_config.target_schema}].[{table_config.target_table}]
                    WHERE {where_clause} AND [{table_config.is_latest_column}] = 1
                    ORDER BY [{table_config.last_modified_column}] DESC
                """)
                
                with self.sql_utils.engine.connect() as conn:
                    latest_record = conn.execute(latest_query, params).fetchone()
                    
                    if latest_record:
                        # Update all others to IsLatest=0
                        update_query = text(f"""
                            UPDATE [{table_config.target_schema}].[{table_config.target_table}]
                            SET [{table_config.is_latest_column}] = 0
                            WHERE {where_clause} 
                                AND [{table_config.is_latest_column}] = 1
                                AND [{table_config.last_modified_column}] != :latest_timestamp
                        """)
                        
                        params["latest_timestamp"] = getattr(latest_record, table_config.last_modified_column)
                        conn.execute(update_query, params)
                        conn.commit()
                        
                        violations_repaired += 1
        
        self.logger.info(
            "SCD integrity repair completed",
            table_name=table_name,
            violations_found=len(violations),
            violations_repaired=violations_repaired,
            dry_run=dry_run
        )
        
        return {
            "violations_found": len(violations),
            "violations_repaired": violations_repaired,
            "dry_run": dry_run,
            "violation_details": [dict(v._mapping) for v in violations]
        }
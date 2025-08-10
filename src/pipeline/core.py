"""Main pipeline class for SQL Server incremental data extraction."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import time
import concurrent.futures
from pathlib import Path
import structlog

from ..config.models import PipelineConfig, PartitionTableConfig, SCDTableConfig, StaticTableConfig
from .sql_utils import SQLServerUtils
from .decimal_handler import DecimalHandler
from .state_manager import StateManager
from .partition_handler import PartitionHandler
from .scd_handler import SCDHandler
from .static_handler import StaticHandler
from ..utils.logging import create_pipeline_logger, PipelineLogger
from ..utils.validation import validate_database_connectivity

logger = structlog.get_logger(__name__)


class IncrementalSQLServerParquetPipeline:
    """
    Main pipeline class for SQL Server incremental data extraction to Parquet.
    
    Supports three table types:
    - Partition tables with SQL Server partition switching
    - SCD Type 2 tables with LastModified and IsLatest logic
    - Static reference tables with hash-based change detection
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Set up logging
        self.logger = create_pipeline_logger(
            name=f"dlt_sync_{config.pipeline_name}",
            config={
                "log_level": config.log_level,
                "log_file": config.log_file,
                "enable_structured_logging": config.enable_structured_logging
            }
        ).bind(pipeline_name=config.pipeline_name)
        
        # Initialize components
        self._initialize_components()
        
        self.logger.info(
            "Pipeline initialized",
            pipeline_name=config.pipeline_name,
            partition_tables=len(config.partition_tables),
            scd_tables=len(config.scd_tables),
            static_tables=len(config.static_tables),
            total_tables=len(config.partition_tables) + len(config.scd_tables) + len(config.static_tables)
        )
    
    def _initialize_components(self) -> None:
        """Initialize pipeline components."""
        try:
            # Initialize SQL Server utilities
            self.sql_utils = SQLServerUtils(self.config.database)
            
            # Test database connectivity
            if not self.sql_utils.test_connection():
                raise RuntimeError("Failed to connect to SQL Server")
            
            # Initialize decimal handler
            self.decimal_handler = DecimalHandler()
            
            # Initialize state manager
            self.state_manager = StateManager(
                self.config.state,
                self.sql_utils,
                self.config.pipeline_name
            )
            
            # Initialize table handlers
            self.partition_handler = PartitionHandler(
                self.sql_utils,
                self.decimal_handler,
                self.state_manager,
                self.config.processing,
                self.logger
            )
            
            self.scd_handler = SCDHandler(
                self.sql_utils,
                self.decimal_handler,
                self.state_manager,
                self.config.processing,
                self.logger
            )
            
            self.static_handler = StaticHandler(
                self.sql_utils,
                self.decimal_handler,
                self.state_manager,
                self.config.processing,
                self.logger
            )
            
            self.logger.info("Pipeline components initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize pipeline components", error=str(e))
            raise
    
    def run(
        self,
        table_types: Optional[List[str]] = None,
        table_names: Optional[List[str]] = None,
        parallel_execution: bool = True
    ) -> Dict[str, Any]:
        """
        Run the incremental data pipeline.
        
        Args:
            table_types: Optional filter for table types (partition, scd, static)
            table_names: Optional filter for specific table names
            parallel_execution: Whether to run table processing in parallel
            
        Returns:
            Pipeline execution results
        """
        self.start_time = datetime.utcnow()
        
        self.logger.info(
            "Starting pipeline execution",
            table_types_filter=table_types,
            table_names_filter=table_names,
            parallel_execution=parallel_execution
        )
        
        try:
            # Validate configuration
            self._validate_configuration()
            
            # Get tables to process
            tables_to_process = self._get_tables_to_process(table_types, table_names)
            
            if not tables_to_process:
                self.logger.warning("No tables to process")
                return self._create_execution_summary([], "no_tables_to_process")
            
            # Process tables
            if parallel_execution and len(tables_to_process) > 1:
                results = self._process_tables_parallel(tables_to_process)
            else:
                results = self._process_tables_sequential(tables_to_process)
            
            # Clean up resources
            self._cleanup_resources()
            
            self.end_time = datetime.utcnow()
            summary = self._create_execution_summary(results, "completed")
            
            self.logger.info(
                "Pipeline execution completed",
                total_duration_seconds=summary["total_duration_seconds"],
                tables_processed=summary["tables_processed"],
                total_records=summary["total_records"],
                errors=summary["error_count"]
            )
            
            return summary
            
        except Exception as e:
            self.end_time = datetime.utcnow()
            self.logger.error(
                "Pipeline execution failed",
                error=str(e),
                duration_seconds=(self.end_time - self.start_time).total_seconds() if self.start_time else 0
            )
            raise
    
    def _validate_configuration(self) -> None:
        """Validate pipeline configuration."""
        self.logger.debug("Validating configuration")
        
        # Validate database connectivity
        try:
            validate_database_connectivity(self.config.database)
        except Exception as e:
            raise RuntimeError(f"Database connectivity validation failed: {str(e)}")
        
        # Validate table configurations
        all_table_names = set()
        
        for table_config in self.config.partition_tables:
            table_name = f"{table_config.schema_name}.{table_config.table_name}"
            if table_name in all_table_names:
                raise ValueError(f"Duplicate table name: {table_name}")
            all_table_names.add(table_name)
        
        for table_config in self.config.scd_tables:
            table_name = f"{table_config.schema_name}.{table_config.table_name}"
            if table_name in all_table_names:
                raise ValueError(f"Duplicate table name: {table_name}")
            all_table_names.add(table_name)
        
        for table_config in self.config.static_tables:
            table_name = f"{table_config.schema_name}.{table_config.table_name}"
            if table_name in all_table_names:
                raise ValueError(f"Duplicate table name: {table_name}")
            all_table_names.add(table_name)
        
        self.logger.debug("Configuration validation completed")
    
    def _get_tables_to_process(
        self,
        table_types: Optional[List[str]],
        table_names: Optional[List[str]]
    ) -> List[Dict[str, Any]]:
        """Get list of tables to process based on filters."""
        tables = []
        
        # Add partition tables
        if not table_types or "partition" in table_types:
            for config in self.config.partition_tables:
                table_name = f"{config.schema_name}.{config.table_name}"
                if not table_names or table_name in table_names:
                    tables.append({
                        "type": "partition",
                        "config": config,
                        "name": table_name
                    })
        
        # Add SCD tables
        if not table_types or "scd" in table_types:
            for config in self.config.scd_tables:
                table_name = f"{config.schema_name}.{config.table_name}"
                if not table_names or table_name in table_names:
                    tables.append({
                        "type": "scd",
                        "config": config,
                        "name": table_name
                    })
        
        # Add static tables
        if not table_types or "static" in table_types:
            for config in self.config.static_tables:
                table_name = f"{config.schema_name}.{config.table_name}"
                if not table_names or table_name in table_names:
                    tables.append({
                        "type": "static",
                        "config": config,
                        "name": table_name
                    })
        
        return tables
    
    def _process_tables_sequential(
        self,
        tables: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process tables sequentially."""
        self.logger.info("Processing tables sequentially", table_count=len(tables))
        
        results = []
        for table_info in tables:
            try:
                result = self._process_single_table(table_info)
                results.append(result)
            except Exception as e:
                error_result = {
                    "table_name": table_info["name"],
                    "table_type": table_info["type"],
                    "success": False,
                    "error": str(e),
                    "total_records": 0,
                    "duration_seconds": 0
                }
                results.append(error_result)
                
                self.logger.error(
                    "Table processing failed",
                    table_name=table_info["name"],
                    table_type=table_info["type"],
                    error=str(e)
                )
        
        return results
    
    def _process_tables_parallel(
        self,
        tables: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process tables in parallel."""
        self.logger.info("Processing tables in parallel", table_count=len(tables))
        
        results = []
        max_workers = min(len(tables), self.config.processing.max_workers)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self._process_single_table, table_info): table_info
                for table_info in tables
            }
            
            for future in concurrent.futures.as_completed(future_to_table):
                table_info = future_to_table[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    error_result = {
                        "table_name": table_info["name"],
                        "table_type": table_info["type"],
                        "success": False,
                        "error": str(e),
                        "total_records": 0,
                        "duration_seconds": 0
                    }
                    results.append(error_result)
                    
                    self.logger.error(
                        "Parallel table processing failed",
                        table_name=table_info["name"],
                        table_type=table_info["type"],
                        error=str(e)
                    )
        
        return results
    
    def _process_single_table(
        self,
        table_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process a single table based on its type."""
        start_time = time.time()
        table_type = table_info["type"]
        config = table_info["config"]
        table_name = table_info["name"]
        
        try:
            if table_type == "partition":
                processing_result = self.partition_handler.process_table(config)
            elif table_type == "scd":
                processing_result = self.scd_handler.process_table(config)
            elif table_type == "static":
                processing_result = self.static_handler.process_table(config)
            else:
                raise ValueError(f"Unknown table type: {table_type}")
            
            duration = time.time() - start_time
            
            result = {
                "table_name": table_name,
                "table_type": table_type,
                "success": True,
                "total_records": processing_result.get("total_records", 0),
                "duration_seconds": duration,
                "processing_details": processing_result
            }
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            return {
                "table_name": table_name,
                "table_type": table_type,
                "success": False,
                "error": str(e),
                "total_records": 0,
                "duration_seconds": duration
            }
    
    def _cleanup_resources(self) -> None:
        """Clean up pipeline resources."""
        try:
            # Clean up old state backups
            if self.config.state.state_retention_days > 0:
                self.state_manager.cleanup_old_state_backups()
            
            # Clean up staging tables for partition tables
            for partition_config in self.config.partition_tables:
                try:
                    self.partition_handler.cleanup_staging_tables(partition_config)
                except Exception as e:
                    self.logger.warning(
                        "Failed to cleanup staging tables",
                        table=f"{partition_config.schema_name}.{partition_config.table_name}",
                        error=str(e)
                    )
            
            self.logger.debug("Resource cleanup completed")
            
        except Exception as e:
            self.logger.warning("Resource cleanup failed", error=str(e))
    
    def _create_execution_summary(
        self,
        results: List[Dict[str, Any]],
        status: str
    ) -> Dict[str, Any]:
        """Create execution summary from results."""
        total_records = sum(result.get("total_records", 0) for result in results)
        successful_tables = [r for r in results if r.get("success", False)]
        failed_tables = [r for r in results if not r.get("success", False)]
        
        total_duration = 0
        if self.start_time and self.end_time:
            total_duration = (self.end_time - self.start_time).total_seconds()
        
        summary = {
            "pipeline_name": self.config.pipeline_name,
            "status": status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_duration_seconds": total_duration,
            "tables_processed": len(results),
            "successful_tables": len(successful_tables),
            "failed_tables": len(failed_tables),
            "total_records": total_records,
            "error_count": len(failed_tables),
            "table_results": results,
            "performance_metrics": {
                "records_per_second": total_records / max(total_duration, 0.001),
                "average_table_duration": total_duration / max(len(results), 1),
                "parallel_processing_used": len(results) > 1 and any(
                    abs(r.get("duration_seconds", 0) - total_duration) > 5 
                    for r in results
                )
            }
        }
        
        # Add table type breakdown
        summary["table_type_breakdown"] = {
            "partition": len([r for r in results if r.get("table_type") == "partition"]),
            "scd": len([r for r in results if r.get("table_type") == "scd"]),
            "static": len([r for r in results if r.get("table_type") == "static"])
        }
        
        return summary
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline status.
        
        Returns:
            Pipeline status information
        """
        try:
            state_status = self.state_manager.get_pipeline_status()
            
            status = {
                "pipeline_name": self.config.pipeline_name,
                "database_connected": self.sql_utils.test_connection(),
                "configuration": {
                    "partition_tables": len(self.config.partition_tables),
                    "scd_tables": len(self.config.scd_tables),
                    "static_tables": len(self.config.static_tables),
                    "total_tables": len(self.config.partition_tables) + len(self.config.scd_tables) + len(self.config.static_tables)
                },
                "state_manager": state_status,
                "last_execution": {
                    "start_time": self.start_time.isoformat() if self.start_time else None,
                    "end_time": self.end_time.isoformat() if self.end_time else None,
                    "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else None
                }
            }
            
            return status
            
        except Exception as e:
            return {
                "pipeline_name": self.config.pipeline_name,
                "error": str(e),
                "status_retrieval_failed": True
            }
    
    def validate_all_tables(self) -> Dict[str, Any]:
        """
        Validate all configured tables.
        
        Returns:
            Validation results for all tables
        """
        self.logger.info("Starting comprehensive table validation")
        
        validation_results = {
            "total_tables": 0,
            "valid_tables": 0,
            "invalid_tables": 0,
            "table_validations": []
        }
        
        # Validate partition tables
        for config in self.config.partition_tables:
            try:
                from ..utils.validation import validate_table_exists, validate_column_exists, validate_partition_configuration
                
                table_name = f"{config.schema_name}.{config.table_name}"
                table_validation = {
                    "table_name": table_name,
                    "table_type": "partition",
                    "valid": True,
                    "issues": []
                }
                
                # Basic table and column validation
                validate_table_exists(self.sql_utils.engine, config.schema_name, config.table_name)
                validate_column_exists(self.sql_utils.engine, config.schema_name, config.table_name, config.partition_column)
                
                # Partition-specific validation
                partition_info = validate_partition_configuration(
                    self.sql_utils.engine, config.schema_name, config.table_name, 
                    config.partition_column, config.partition_function
                )
                
                table_validation["partition_info"] = partition_info
                validation_results["valid_tables"] += 1
                
            except Exception as e:
                table_validation = {
                    "table_name": f"{config.schema_name}.{config.table_name}",
                    "table_type": "partition",
                    "valid": False,
                    "issues": [str(e)]
                }
                validation_results["invalid_tables"] += 1
            
            validation_results["table_validations"].append(table_validation)
            validation_results["total_tables"] += 1
        
        # Validate SCD tables
        for config in self.config.scd_tables:
            try:
                table_name = f"{config.schema_name}.{config.table_name}"
                scd_validation = self.scd_handler._validate_scd_configuration(config)
                
                table_validation = {
                    "table_name": table_name,
                    "table_type": "scd",
                    "valid": scd_validation["is_valid"],
                    "issues": scd_validation.get("errors", []),
                    "scd_info": scd_validation
                }
                
                if scd_validation["is_valid"]:
                    validation_results["valid_tables"] += 1
                else:
                    validation_results["invalid_tables"] += 1
                
            except Exception as e:
                table_validation = {
                    "table_name": f"{config.schema_name}.{config.table_name}",
                    "table_type": "scd",
                    "valid": False,
                    "issues": [str(e)]
                }
                validation_results["invalid_tables"] += 1
            
            validation_results["table_validations"].append(table_validation)
            validation_results["total_tables"] += 1
        
        # Validate static tables
        for config in self.config.static_tables:
            try:
                table_name = f"{config.schema_name}.{config.table_name}"
                integrity_validation = self.static_handler.validate_static_table_integrity(config)
                
                table_validation = {
                    "table_name": table_name,
                    "table_type": "static",
                    "valid": integrity_validation["validation_passed"],
                    "issues": integrity_validation.get("data_quality_issues", []),
                    "integrity_info": integrity_validation
                }
                
                if integrity_validation["validation_passed"]:
                    validation_results["valid_tables"] += 1
                else:
                    validation_results["invalid_tables"] += 1
                
            except Exception as e:
                table_validation = {
                    "table_name": f"{config.schema_name}.{config.table_name}",
                    "table_type": "static",
                    "valid": False,
                    "issues": [str(e)]
                }
                validation_results["invalid_tables"] += 1
            
            validation_results["table_validations"].append(table_validation)
            validation_results["total_tables"] += 1
        
        self.logger.info(
            "Table validation completed",
            total_tables=validation_results["total_tables"],
            valid_tables=validation_results["valid_tables"],
            invalid_tables=validation_results["invalid_tables"]
        )
        
        return validation_results
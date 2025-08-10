"""Basic usage example for the SQL Server Incremental Data Pipeline."""

from pathlib import Path
from src.config.models import (
    PipelineConfig, DatabaseConfig, ProcessingConfig, StateConfig,
    PartitionTableConfig, SCDTableConfig, StaticTableConfig
)
from src.pipeline.core import IncrementalSQLServerParquetPipeline


def main():
    """Demonstrate basic pipeline usage."""
    
    # Database configuration
    database_config = DatabaseConfig(
        server="localhost",
        database="AdventureWorks2019",
        integrated_security=True,  # Use Windows authentication
        trust_server_certificate=True
    )
    
    # Processing configuration
    processing_config = ProcessingConfig(
        batch_size=5000,
        max_workers=3,
        retry_attempts=3,
        enable_sql_server_partition_switching=True
    )
    
    # State management configuration
    state_config = StateConfig(
        state_directory=Path("./pipeline_state"),
        parquet_directory=Path("./data/parquet"),
        enable_self_healing=True,
        state_retention_days=30
    )
    
    # Configure partition tables (e.g., sales data partitioned by date)
    partition_tables = [
        PartitionTableConfig(
            table_name="SalesOrderHeader",
            schema_name="Sales",
            target_table="sales_order_header",
            target_schema="analytics",
            partition_column="OrderDate",
            batch_size=10000
        )
    ]
    
    # Configure SCD Type 2 tables (e.g., customer data with history)
    scd_tables = [
        SCDTableConfig(
            table_name="Customer",
            schema_name="Sales",
            target_table="customer_history",
            target_schema="analytics",
            business_key_columns=["CustomerID"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsLatest",
            effective_date_column="EffectiveDate",
            end_date_column="EndDate"
        )
    ]
    
    # Configure static reference tables (e.g., product catalog)
    static_tables = [
        StaticTableConfig(
            table_name="Product",
            schema_name="Production",
            target_table="product_reference",
            target_schema="analytics",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace",
            force_reload_after_days=7
        )
    ]
    
    # Main pipeline configuration
    config = PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=partition_tables,
        scd_tables=scd_tables,
        static_tables=static_tables,
        pipeline_name="adventureworks_incremental",
        destination="parquet",
        log_level="INFO"
    )
    
    # Initialize and run pipeline
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    print("Starting pipeline execution...")
    
    # Run all tables
    results = pipeline.run(parallel_execution=True)
    
    print(f"Pipeline completed with status: {results['status']}")
    print(f"Tables processed: {results['tables_processed']}")
    print(f"Total records: {results['total_records']}")
    print(f"Duration: {results['total_duration_seconds']:.2f} seconds")
    print(f"Records per second: {results['performance_metrics']['records_per_second']:.2f}")
    
    # Print table-level results
    print("\nTable-level results:")
    for table_result in results['table_results']:
        status = "✓" if table_result['success'] else "✗"
        print(f"  {status} {table_result['table_name']} ({table_result['table_type']}): "
              f"{table_result['total_records']} records in {table_result['duration_seconds']:.1f}s")
        
        if not table_result['success']:
            print(f"    Error: {table_result['error']}")
    
    # Get pipeline status
    print("\nPipeline status:")
    status = pipeline.get_pipeline_status()
    print(f"  Database connected: {status['database_connected']}")
    print(f"  Total configured tables: {status['configuration']['total_tables']}")
    print(f"  State manager tables: {status['state_manager']['total_tables']}")


def run_specific_table_types():
    """Example of running only specific table types."""
    
    # Use same configuration as main example
    config = get_sample_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Run only static tables
    print("Running only static reference tables...")
    results = pipeline.run(
        table_types=["static"],
        parallel_execution=False
    )
    
    print(f"Static tables processed: {results['table_type_breakdown']['static']}")


def run_specific_tables():
    """Example of running only specific tables by name."""
    
    config = get_sample_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Run only specific tables
    tables_to_run = ["Sales.Customer", "Production.Product"]
    
    print(f"Running specific tables: {tables_to_run}")
    results = pipeline.run(
        table_names=tables_to_run,
        parallel_execution=True
    )
    
    print(f"Specified tables processed: {len(results['table_results'])}")


def validate_configuration():
    """Example of validating all table configurations."""
    
    config = get_sample_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    print("Validating all table configurations...")
    validation_results = pipeline.validate_all_tables()
    
    print(f"Total tables: {validation_results['total_tables']}")
    print(f"Valid tables: {validation_results['valid_tables']}")
    print(f"Invalid tables: {validation_results['invalid_tables']}")
    
    # Show validation details
    for table_validation in validation_results['table_validations']:
        status = "✓" if table_validation['valid'] else "✗"
        print(f"  {status} {table_validation['table_name']} ({table_validation['table_type']})")
        
        if not table_validation['valid']:
            for issue in table_validation['issues']:
                print(f"    Issue: {issue}")


def get_sample_config() -> PipelineConfig:
    """Get sample configuration for examples."""
    
    database_config = DatabaseConfig(
        server="localhost",
        database="AdventureWorks2019",
        integrated_security=True,
        trust_server_certificate=True
    )
    
    return PipelineConfig(
        database=database_config,
        processing=ProcessingConfig(),
        state=StateConfig(),
        partition_tables=[
            PartitionTableConfig(
                table_name="SalesOrderHeader",
                schema_name="Sales",
                target_table="sales_order_header",
                target_schema="analytics",
                partition_column="OrderDate"
            )
        ],
        scd_tables=[
            SCDTableConfig(
                table_name="Customer",
                schema_name="Sales",
                target_table="customer_history", 
                target_schema="analytics",
                business_key_columns=["CustomerID"],
                last_modified_column="ModifiedDate",
                is_latest_column="IsLatest"
            )
        ],
        static_tables=[
            StaticTableConfig(
                table_name="Product",
                schema_name="Production",
                target_table="product_reference",
                target_schema="analytics",
                hash_algorithm="hashbytes_sha256",
                load_strategy="replace"
            )
        ],
        pipeline_name="sample_pipeline"
    )


if __name__ == "__main__":
    # Run main example
    main()
    
    print("\n" + "="*50)
    
    # Run additional examples
    run_specific_table_types()
    
    print("\n" + "="*50)
    
    run_specific_tables()
    
    print("\n" + "="*50)
    
    validate_configuration()
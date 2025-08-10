"""Configuration examples for different use cases and environments."""

import os
from pathlib import Path
from typing import Dict, Any
from src.config.models import (
    PipelineConfig, DatabaseConfig, ProcessingConfig, StateConfig,
    PartitionTableConfig, SCDTableConfig, StaticTableConfig
)


def production_config() -> PipelineConfig:
    """Production-ready configuration with comprehensive settings."""
    
    # Production database configuration with connection pooling
    database_config = DatabaseConfig(
        server=os.getenv("PROD_SQL_SERVER", "prod-sql.company.com"),
        database=os.getenv("PROD_DATABASE", "DataWarehouse"),
        username=os.getenv("PROD_SQL_USER"),  # Use SQL authentication in production
        password=os.getenv("PROD_SQL_PASSWORD"),
        integrated_security=False,
        port=1433,
        driver="ODBC Driver 18 for SQL Server",
        trust_server_certificate=False,  # Use proper certificates in production
        connection_timeout=30,
        command_timeout=600  # Longer timeout for large operations
    )
    
    # Optimized processing configuration for production
    processing_config = ProcessingConfig(
        batch_size=50000,  # Larger batches for better throughput
        max_workers=8,  # More workers for parallel processing
        memory_limit_mb=2048,  # Higher memory limit
        retry_attempts=5,  # More retries for reliability
        retry_delay_seconds=60,
        partition_parallel_limit=4,  # More parallel partition operations
        enable_sql_server_partition_switching=True,
        validate_precision_end_to_end=True  # Full validation in production
    )
    
    # Production state management
    state_config = StateConfig(
        state_directory=Path("/opt/dlt-sync/state"),
        parquet_directory=Path("/data/warehouse/parquet"),
        enable_self_healing=True,
        state_retention_days=90,  # Longer retention for production
        validate_state_consistency=True,
        backup_state_before_run=True
    )
    
    # Production partition tables - large transaction tables
    partition_tables = [
        PartitionTableConfig(
            table_name="Orders",
            schema_name="sales",
            target_table="orders",
            target_schema="warehouse",
            partition_column="OrderDate",
            batch_size=100000,
            enable_partition_switching=True,
            partition_function="OrderDateRangeFunction",
            partition_scheme="OrderDateRangeScheme"
        ),
        PartitionTableConfig(
            table_name="OrderDetails", 
            schema_name="sales",
            target_table="order_details",
            target_schema="warehouse",
            partition_column="OrderDate",
            batch_size=200000,
            enable_partition_switching=True,
            custom_where_clause="IsDeleted = 0"  # Filter out soft-deleted records
        ),
        PartitionTableConfig(
            table_name="Transactions",
            schema_name="finance",
            target_table="transactions",
            target_schema="warehouse", 
            partition_column="TransactionDate",
            batch_size=75000
        )
    ]
    
    # Production SCD tables - master data with history
    scd_tables = [
        SCDTableConfig(
            table_name="Customers",
            schema_name="crm",
            target_table="customers",
            target_schema="warehouse",
            business_key_columns=["CustomerID"],
            last_modified_column="LastModified",
            is_latest_column="IsCurrent",
            effective_date_column="ValidFrom",
            end_date_column="ValidTo",
            batch_size=25000
        ),
        SCDTableConfig(
            table_name="Products",
            schema_name="catalog",
            target_table="products",
            target_schema="warehouse",
            business_key_columns=["ProductSKU"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsActive",
            batch_size=10000,
            custom_where_clause="Status != 'DELETED'"
        ),
        SCDTableConfig(
            table_name="Employees",
            schema_name="hr",
            target_table="employees",
            target_schema="warehouse",
            business_key_columns=["EmployeeID"],
            last_modified_column="LastUpdated",
            is_latest_column="IsCurrentEmployee",
            effective_date_column="EffectiveDate",
            end_date_column="EndDate"
        )
    ]
    
    # Production static tables - reference data
    static_tables = [
        StaticTableConfig(
            table_name="Countries",
            schema_name="reference",
            target_table="countries",
            target_schema="warehouse",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace",
            force_reload_after_days=30
        ),
        StaticTableConfig(
            table_name="ProductCategories",
            schema_name="catalog",
            target_table="product_categories",
            target_schema="warehouse",
            hash_algorithm="hashbytes_sha256",
            load_strategy="upsert",
            primary_key_columns=["CategoryID"],
            force_reload_after_days=7
        ),
        StaticTableConfig(
            table_name="CurrencyRates",
            schema_name="finance",
            target_table="currency_rates",
            target_schema="warehouse",
            hash_algorithm="hashbytes_md5",
            load_strategy="replace",
            force_reload_after_days=1  # Daily refresh for rates
        )
    ]
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=partition_tables,
        scd_tables=scd_tables,
        static_tables=static_tables,
        pipeline_name="production_warehouse_etl",
        destination="parquet",
        destination_config={
            "file_format": "parquet",
            "compression": "snappy"
        },
        log_level="INFO",
        enable_structured_logging=True,
        log_file=Path("/var/log/dlt-sync/production.log")
    )


def development_config() -> PipelineConfig:
    """Development configuration with smaller batches and more logging."""
    
    database_config = DatabaseConfig(
        server="dev-sql.company.local",
        database="DevWarehouse",
        integrated_security=True,
        trust_server_certificate=True,
        connection_timeout=15,
        command_timeout=120
    )
    
    processing_config = ProcessingConfig(
        batch_size=1000,  # Smaller batches for development
        max_workers=2,
        memory_limit_mb=512,
        retry_attempts=2,
        retry_delay_seconds=10,
        partition_parallel_limit=1,
        enable_sql_server_partition_switching=False,  # Simpler setup for dev
        validate_precision_end_to_end=True
    )
    
    state_config = StateConfig(
        state_directory=Path("./dev_state"),
        parquet_directory=Path("./dev_data"),
        enable_self_healing=True,
        state_retention_days=7,
        validate_state_consistency=True,
        backup_state_before_run=True
    )
    
    # Simplified table configurations for development
    partition_tables = [
        PartitionTableConfig(
            table_name="TestOrders",
            schema_name="test",
            target_table="orders",
            target_schema="dev",
            partition_column="OrderDate",
            batch_size=500
        )
    ]
    
    scd_tables = [
        SCDTableConfig(
            table_name="TestCustomers",
            schema_name="test",
            target_table="customers", 
            target_schema="dev",
            business_key_columns=["CustomerID"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsLatest",
            batch_size=100
        )
    ]
    
    static_tables = [
        StaticTableConfig(
            table_name="TestProducts",
            schema_name="test",
            target_table="products",
            target_schema="dev",
            hash_algorithm="checksum",  # Faster for development
            load_strategy="replace"
        )
    ]
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=partition_tables,
        scd_tables=scd_tables,
        static_tables=static_tables,
        pipeline_name="development_pipeline",
        log_level="DEBUG",  # More verbose logging for development
        enable_structured_logging=True
    )


def high_volume_partition_config() -> PipelineConfig:
    """Configuration optimized for high-volume partition tables."""
    
    database_config = DatabaseConfig(
        server=os.getenv("HV_SQL_SERVER"),
        database=os.getenv("HV_DATABASE"),
        username=os.getenv("HV_SQL_USER"),
        password=os.getenv("HV_SQL_PASSWORD"),
        integrated_security=False,
        connection_timeout=45,
        command_timeout=1800  # 30 minutes for large operations
    )
    
    # Aggressive processing configuration for high volume
    processing_config = ProcessingConfig(
        batch_size=500000,  # Very large batches
        max_workers=12,
        memory_limit_mb=4096,  # 4GB per worker
        retry_attempts=3,
        retry_delay_seconds=120,
        partition_parallel_limit=6,  # High parallelism
        enable_sql_server_partition_switching=True,
        validate_precision_end_to_end=False  # Skip validation for performance
    )
    
    state_config = StateConfig(
        state_directory=Path("/fast-storage/dlt-sync/state"),
        parquet_directory=Path("/fast-storage/data/parquet"),
        enable_self_healing=True,
        state_retention_days=30,
        validate_state_consistency=False,  # Skip for performance
        backup_state_before_run=False  # Skip backup for performance
    )
    
    # Only partition tables for high-volume scenario
    partition_tables = [
        PartitionTableConfig(
            table_name="LargeTransactionLog",
            schema_name="audit",
            target_table="transaction_log",
            target_schema="analytics",
            partition_column="LogDate", 
            batch_size=1000000,  # 1M records per batch
            enable_partition_switching=True,
            partition_function="LogDateFunction",
            partition_scheme="LogDateScheme"
        ),
        PartitionTableConfig(
            table_name="EventStream",
            schema_name="events",
            target_table="event_stream",
            target_schema="analytics",
            partition_column="EventTimestamp",
            batch_size=750000,
            enable_partition_switching=True
        )
    ]
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=partition_tables,
        scd_tables=[],  # No SCD tables in high-volume scenario
        static_tables=[],  # No static tables
        pipeline_name="high_volume_partition_etl",
        destination="parquet",
        destination_config={
            "file_format": "parquet",
            "compression": "lz4",  # Fast compression
            "row_group_size": 1000000
        },
        log_level="WARNING",  # Minimal logging for performance
        enable_structured_logging=False
    )


def scd_focused_config() -> PipelineConfig:
    """Configuration focused on SCD Type 2 tables with comprehensive history tracking."""
    
    database_config = DatabaseConfig(
        server=os.getenv("SCD_SQL_SERVER"),
        database=os.getenv("SCD_DATABASE"), 
        integrated_security=True,
        trust_server_certificate=True
    )
    
    processing_config = ProcessingConfig(
        batch_size=10000,
        max_workers=4,
        retry_attempts=5,  # More retries for SCD integrity
        validate_precision_end_to_end=True
    )
    
    state_config = StateConfig(
        state_directory=Path("./scd_state"),
        parquet_directory=Path("./scd_data"),
        enable_self_healing=True,
        validate_state_consistency=True  # Critical for SCD integrity
    )
    
    # Comprehensive SCD Type 2 configuration
    scd_tables = [
        SCDTableConfig(
            table_name="CustomerMaster",
            schema_name="master",
            target_table="customer_history",
            target_schema="dwh",
            business_key_columns=["CustomerNumber"],
            last_modified_column="LastModified",
            is_latest_column="IsCurrent",
            effective_date_column="ValidFrom",
            end_date_column="ValidTo"
        ),
        SCDTableConfig(
            table_name="ProductMaster",
            schema_name="master",
            target_table="product_history",
            target_schema="dwh",
            business_key_columns=["ProductSKU"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsActive",
            effective_date_column="EffectiveDate",
            end_date_column="ExpiryDate"
        ),
        SCDTableConfig(
            table_name="SupplierMaster",
            schema_name="master",
            target_table="supplier_history",
            target_schema="dwh",
            business_key_columns=["SupplierID"],
            last_modified_column="UpdatedAt",
            is_latest_column="IsCurrentSupplier",
            effective_date_column="ValidFromDate",
            end_date_column="ValidToDate",
            custom_where_clause="Status = 'ACTIVE'"
        ),
        SCDTableConfig(
            table_name="LocationMaster", 
            schema_name="master",
            target_table="location_history",
            target_schema="dwh",
            business_key_columns=["LocationCode", "RegionCode"],  # Composite business key
            last_modified_column="LastUpdated",
            is_latest_column="IsCurrent"
        )
    ]
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=[],
        scd_tables=scd_tables,
        static_tables=[],
        pipeline_name="scd_master_data_pipeline",
        log_level="INFO",
        enable_structured_logging=True
    )


def get_config_from_environment() -> PipelineConfig:
    """Load configuration from environment variables."""
    
    # Database configuration from environment
    database_config = DatabaseConfig(
        server=os.getenv("DLT_SYNC_SERVER", "localhost"),
        database=os.getenv("DLT_SYNC_DATABASE", "DefaultDB"),
        username=os.getenv("DLT_SYNC_USERNAME"),
        password=os.getenv("DLT_SYNC_PASSWORD"),
        integrated_security=os.getenv("DLT_SYNC_INTEGRATED_AUTH", "true").lower() == "true",
        port=int(os.getenv("DLT_SYNC_PORT", "1433")),
        trust_server_certificate=os.getenv("DLT_SYNC_TRUST_CERT", "true").lower() == "true"
    )
    
    # Processing configuration from environment
    processing_config = ProcessingConfig(
        batch_size=int(os.getenv("DLT_SYNC_BATCH_SIZE", "10000")),
        max_workers=int(os.getenv("DLT_SYNC_MAX_WORKERS", "4")),
        retry_attempts=int(os.getenv("DLT_SYNC_RETRY_ATTEMPTS", "3")),
        enable_sql_server_partition_switching=os.getenv("DLT_SYNC_PARTITION_SWITCHING", "true").lower() == "true"
    )
    
    # State configuration from environment
    state_config = StateConfig(
        state_directory=Path(os.getenv("DLT_SYNC_STATE_DIR", "./state")),
        parquet_directory=Path(os.getenv("DLT_SYNC_DATA_DIR", "./data")),
        state_retention_days=int(os.getenv("DLT_SYNC_STATE_RETENTION_DAYS", "30"))
    )
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=[],  # Define in separate config files
        scd_tables=[],
        static_tables=[],
        pipeline_name=os.getenv("DLT_SYNC_PIPELINE_NAME", "env_pipeline"),
        log_level=os.getenv("DLT_SYNC_LOG_LEVEL", "INFO"),
        log_file=Path(os.getenv("DLT_SYNC_LOG_FILE")) if os.getenv("DLT_SYNC_LOG_FILE") else None
    )


def create_config_template() -> Dict[str, Any]:
    """Create a configuration template for easy customization."""
    
    template = {
        "database": {
            "server": "YOUR_SQL_SERVER",
            "database": "YOUR_DATABASE",
            "integrated_security": True,
            "trust_server_certificate": True,
            "connection_timeout": 30,
            "command_timeout": 300
        },
        "processing": {
            "batch_size": 10000,
            "max_workers": 4,
            "retry_attempts": 3,
            "enable_sql_server_partition_switching": True
        },
        "state": {
            "state_directory": "./pipeline_state",
            "parquet_directory": "./data",
            "enable_self_healing": True,
            "state_retention_days": 30
        },
        "partition_tables": [
            {
                "table_name": "YOUR_PARTITION_TABLE",
                "schema_name": "dbo",
                "target_table": "target_partition_table",
                "target_schema": "analytics",
                "partition_column": "DateColumn",
                "batch_size": 50000
            }
        ],
        "scd_tables": [
            {
                "table_name": "YOUR_SCD_TABLE",
                "schema_name": "dbo", 
                "target_table": "target_scd_table",
                "target_schema": "analytics",
                "business_key_columns": ["ID"],
                "last_modified_column": "ModifiedDate",
                "is_latest_column": "IsLatest"
            }
        ],
        "static_tables": [
            {
                "table_name": "YOUR_STATIC_TABLE",
                "schema_name": "dbo",
                "target_table": "target_static_table",
                "target_schema": "analytics",
                "hash_algorithm": "hashbytes_sha256",
                "load_strategy": "replace"
            }
        ],
        "pipeline_name": "your_pipeline_name",
        "log_level": "INFO"
    }
    
    return template


if __name__ == "__main__":
    """Examples of different configurations."""
    
    print("=== Production Configuration ===")
    prod_config = production_config()
    print(f"Pipeline: {prod_config.pipeline_name}")
    print(f"Database: {prod_config.database.server}/{prod_config.database.database}")
    print(f"Total tables: {len(prod_config.partition_tables) + len(prod_config.scd_tables) + len(prod_config.static_tables)}")
    
    print("\n=== Development Configuration ===")
    dev_config = development_config() 
    print(f"Pipeline: {dev_config.pipeline_name}")
    print(f"Database: {dev_config.database.server}/{dev_config.database.database}")
    print(f"Log level: {dev_config.log_level}")
    
    print("\n=== High Volume Configuration ===") 
    hv_config = high_volume_partition_config()
    print(f"Pipeline: {hv_config.pipeline_name}")
    print(f"Batch size: {hv_config.processing.batch_size}")
    print(f"Max workers: {hv_config.processing.max_workers}")
    
    print("\n=== SCD Focused Configuration ===")
    scd_config = scd_focused_config()
    print(f"Pipeline: {scd_config.pipeline_name}")
    print(f"SCD tables: {len(scd_config.scd_tables)}")
    
    print("\n=== Environment Configuration ===")
    env_config = get_config_from_environment()
    print(f"Pipeline: {env_config.pipeline_name}")
    print(f"Batch size: {env_config.processing.batch_size}")
    
    print("\n=== Configuration Template ===")
    template = create_config_template()
    print("Template created with sample values for all configuration options")
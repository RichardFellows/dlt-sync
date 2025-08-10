# SQL Server Incremental Data Pipeline Requirements

## Overview
Create a robust Python-based data pipeline that extracts partitioned data from a source SQL Server database, stores it as Parquet files for reuse, and loads it incrementally into a target SQL Server database using the `dlt` (data load tool) library.

## Core Requirements

### 1. Pipeline Architecture
- **Framework**: Must use the `dlt` library for data extraction and loading
- **Language**: Python 3.8+
- **Storage Format**: Apache Parquet for intermediate storage
- **State Management**: Self-healing state detection (no external state files)
- **Table Types**: Support three distinct table patterns:
  1. **Partition-Based Tables**: Tables with configurable partition columns for date-based processing
  2. **SCD Type 2 Tables**: Tables with `LastModified` datetime and `IsLatest` bit fields
  3. **Static Reference Tables**: Small tables with generally static data requiring full synchronization

### 2. Data Partitioning Strategies

#### COBID-Partitioned Tables
- **Partition Column**: Tables are partitioned by an integer column representing reporting dates (typically YYYYMMDD format)
- **Configurable Column Name**: The partition column name is configurable per table (e.g., `COBID`, `ReportingDate`, `BusinessDate`, `ProcessDate`)
- **Incremental Processing**: Pipeline must detect and process only new partition values since the last successful run
- **Initial Load Strategy**: On first run, process only the last N partition values (configurable, default: 10) to avoid processing years of historical data
- **SQL Server Native Partitioning**: Support for tables using SQL Server's native partitioning with partition switching capabilities

#### SCD Type 2 Tables
- **Change Tracking Columns**: 
  - `LastModified` (datetime): Timestamp when the record was last modified
  - `IsLatest` (bit): Flag indicating if this is the current/latest version of the record
- **Business Key**: Tables have one or more business key columns that identify the entity
- **Incremental Processing**: Extract records modified since last pipeline run based on `LastModified` timestamp
- **SCD Logic**: Handle IsLatest flag updates when new versions of existing records are inserted

#### Static Reference Tables
- **Change Detection**: Detect changes by comparing source data against last known state
- **Full Extraction**: Each extract contains the complete recordset (not incremental)
- **Conditional Parquet Creation**: Only create new Parquet files if changes are detected
- **Merge Strategy**: Use merge/upsert operations to synchronize destination with source
- **Hash-Based Detection**: Use table-level or row-level hashing to efficiently detect changes
- **Small Table Optimization**: Optimized for tables with typically fewer than 100K records

### 3. State Detection and Recovery

#### For COBID-Partitioned Tables
The pipeline must be **self-healing** and determine the last processed partition value by examining:
- **Parquet Files**: Scan organized Parquet files to find highest partition value processed
- **Target Database**: Query target database for maximum partition value per table
- **Consistency Logic**: Use the minimum of both values to ensure data consistency

#### For SCD Type 2 Tables
The pipeline must determine the last processed timestamp by examining:
- **Parquet Files**: Scan Parquet file metadata or contents to find latest `LastModified` timestamp processed
- **Target Database**: Query target database for maximum `LastModified` timestamp per table
- **Consistency Logic**: Use the minimum of both values to ensure no missed records
- **Initial Load Strategy**: On first run, process only records from the last N days (configurable, default: 30 days)

#### For Static Reference Tables
The pipeline must determine if table contents have changed by examining:
- **Source Table Hash**: Calculate hash of entire source table content
- **Last Parquet Hash**: Retrieve hash from last created Parquet file metadata
- **Target Database State**: Optionally verify target database matches last known state
- **Change Detection**: Only proceed with extraction if hashes differ
- **Metadata Tracking**: Store extraction timestamp and content hash in Parquet metadata

#### Universal Requirements
- **No State Files**: Do not rely on external state files that can be lost or corrupted
- **Self-Healing**: Automatically detect and recover from inconsistent states

### 4. High-Precision Decimal Handling
- **Requirement**: Must preserve exact precision for high-precision decimal columns (e.g., `DECIMAL(38,18)`)
- **Implementation**: Use Arrow's native `decimal128` type for values up to 38 digits precision
- **Validation**: Include validation to verify decimal precision is maintained end-to-end
- **Type Mapping**: Handle SQL Server MONEY, SMALLMONEY as appropriate decimal types

### 5. Data Flow Architecture

#### Extraction Phase
1. **Table Type Detection**: Automatically detect whether tables use COBID partitioning or SCD Type 2 pattern
2. **Column Metadata Detection**: Query `INFORMATION_SCHEMA.COLUMNS` to get precise data types, precision, and scale
3. **Arrow Schema Building**: Create proper Arrow schema with `decimal128` types for high-precision decimals
4. **Batch Processing**: Extract data in configurable batches (default: 10,000 rows) to manage memory

##### For COBID-Partitioned Tables
5. **Partition-Specific Queries**: Extract data using `WHERE {partition_column} = {target_partition_value}` filters
6. **Direct Arrow Conversion**: Convert SQL Server data directly to Arrow format without string intermediates

##### For SCD Type 2 Tables
5. **Timestamp-Based Queries**: Extract data using `WHERE LastModified > {last_processed_timestamp}` filters
6. **Business Key Identification**: Determine business key columns for SCD processing
7. **Change Detection**: Identify new versions of existing records vs. entirely new records

##### For Static Reference Tables
5. **Change Detection Queries**: Calculate hash of entire table content using `CHECKSUM` or similar
6. **Full Table Extraction**: Extract complete recordset when changes are detected
7. **Conditional Processing**: Skip extraction if no changes detected since last run

#### Storage Phase
1. **Parquet Organization**: 
   - **Partition Tables**: Store in `parquet_data/organized/{partition_column}_{partition_value}/`
   - **SCD Tables**: Store in `parquet_data/organized/scd_{table_name}/{YYYY-MM-DD}/`
   - **Static Tables**: Store in `parquet_data/organized/static_{table_name}/`
2. **File Naming**: 
   - **Partition**: `{table_name}_{partition_column}_{partition_value}.parquet`
   - **SCD**: `{table_name}_scd_{YYYY-MM-DD-HH-MM-SS}.parquet`
   - **Static**: `{table_name}_static_{YYYY-MM-DD-HH-MM-SS}.parquet`
3. **Precision Preservation**: Store decimal data using Arrow's native decimal types
4. **Metadata Storage**: Include processing timestamp metadata in Parquet files
   - **Static Tables**: Include content hash and row count in metadata

#### Loading Phase - COBID Tables
1. **Load Strategy Detection**: Determine whether to use partition switching or direct load
2. **Direct Load**: Use DLT's `write_disposition="append"` for incremental loading
3. **Partition Switching**: Use staging tables and SQL Server partition switching for large tables
4. **Batch Loading**: Process multiple Parquet files efficiently
5. **Type Conversion**: Convert Arrow decimals appropriately for SQL Server insertion

#### Loading Phase - SCD Type 2 Tables
1. **Upsert Logic**: Implement proper SCD Type 2 handling:
   - Insert new records (new business keys)
   - Insert new versions of existing records
   - Update `IsLatest=0` for superseded records
2. **Transaction Handling**: Ensure atomicity of SCD operations
3. **Business Key Matching**: Match records based on configured business key columns
4. **Timestamp Validation**: Ensure newer records don't have older LastModified timestamps

#### Loading Phase - Static Reference Tables
1. **Merge/Upsert Logic**: Implement table synchronization:
   - Compare source records with target table
   - Insert new records
   - Update changed records  
   - Delete records not present in source (optional)
2. **Primary Key Matching**: Use table's primary key or configured key columns for matching
3. **Full Replacement Option**: Alternative strategy to truncate and reload entire table
4. **Transaction Handling**: Ensure atomicity of merge operations

### 6. SQL Server Native Partition Support

#### Partition Detection
- **Automatic Detection**: Query system tables to detect if target table uses SQL Server native partitioning
- **Partition Metadata**: Retrieve partition scheme, function, and boundary values
- **Partition Mapping**: Map COBID values to SQL Server partition numbers

#### Partition Switching Strategy
- **Decision Logic**: 
  - Use partition switching for SQL Server partitioned tables when configured
  - Automatically determine best approach based on table size and partition state
  - Fall back to direct load for non-partitioned tables
- **Staging Table Management**:
  - Create staging tables with identical structure to target
  - Apply appropriate check constraints for partition alignment
  - Match indexes and compression settings

#### Partition Switch Implementation
- **Pre-Switch Validation**: Verify staging table meets all requirements for switching
- **Switch Operation**: Execute atomic partition switch using `ALTER TABLE ... SWITCH`
- **Archive Management**: Optional archiving of replaced partitions before switching
- **Cleanup**: Remove staging tables after successful switch
- **Rollback Capability**: Maintain ability to rollback failed switches

#### Performance Optimizations for Partitioned Tables
- **Parallel Loading**: Support loading multiple partitions in parallel using separate staging tables
- **Index Management**: Disable/rebuild indexes on staging tables as needed
- **Statistics Updates**: Update statistics on staging tables before switching
- **Compression**: Apply appropriate compression to staging tables

### 7. Configuration Requirements
- **Connection Strings**: Support for SQL Server connection strings with various authentication methods
- **Table Configuration**: Support for mixed table types in the same pipeline:
  ```python
  table_config = {
      "partition_tables": {
          "daily_transactions": {
              "partition_column": "COBID",
              "initial_load_count": 10,
              "sql_server_partitioned": True,
              "partition_switching": {
                  "enabled": True,
                  "staging_table_suffix": "_STAGE",
                  "switch_method": "auto",  # auto, always_stage, or direct
                  "validate_schema": True,
                  "compress_staging": True,
                  "parallel_load": True,
                  "max_parallel": 4
              }
          },
          "daily_balances": {
              "partition_column": "ReportingDate", 
              "initial_load_count": 5,
              "sql_server_partitioned": False
          },
          "risk_metrics": {
              "partition_column": "BusinessDate",
              "initial_load_count": 15,
              "sql_server_partitioned": True,
              "partition_switching": {
                  "enabled": True,
                  "staging_table_suffix": "_TRANSFORM",
                  "switch_method": "always_stage"
              }
          }
      },
      "scd_tables": {
          "customer_dim": {
              "business_keys": ["customer_id"],
              "initial_load_days": 30
          },
          "product_dim": {
              "business_keys": ["product_code", "version"],
              "initial_load_days": 7
          }
      },
      "static_tables": {
          "currency_codes": {
              "primary_keys": ["currency_code"],
              "merge_strategy": "upsert"
          },
          "country_lookup": {
              "primary_keys": ["country_id"],
              "merge_strategy": "upsert",
              "allow_deletes": False
          },
          "product_categories": {
              "primary_keys": ["category_id"],
              "merge_strategy": "replace"
          }
      }
  }
  ```
- **Configurable Parameters**:
  - Source and target connection strings
  - Parquet storage path
  - Per-table partition column names and initial load counts
  - SQL Server partition switching configuration
  - Initial load days for SCD tables
  - Primary keys and merge strategies for static tables
  - Batch size for processing
  - Target schema name

### 8. Logging and Monitoring
- **Comprehensive Logging**: Log all major operations with timestamps
- **Progress Tracking**: Report batch progress and row counts
- **Error Handling**: Graceful error handling with detailed error messages
- **Status Reporting**: Provide detailed status of pipeline state for each table
- **Partition Switch Logging**: Detailed logging of partition switch operations

### 9. Analysis and Validation Features

#### Missing Data Analysis
Provide method to analyze:
- **Partition Tables**: Partition values available in source vs. Parquet vs. target database
- **SCD Tables**: LastModified timestamp ranges and gaps
- **Static Tables**: Content hash mismatches between source, Parquet, and target
- **SQL Server Partitions**: Partition alignment and boundary validation
- Identification of missing or inconsistent data

#### Precision Validation
- Compare sample high-precision decimal values between source and target
- Report any precision loss
- Validate that decimal columns maintain exact values

#### Pipeline Status
- **Partition Tables**: Last processed partition value per table
- **SCD Tables**: Last processed LastModified timestamp per table
- **Static Tables**: Last content hash and extraction timestamp per table
- **SQL Server Partitions**: Status of partition switching operations
- Pending data to process
- Consistency status between Parquet and target database

#### SCD Integrity Validation
- Verify that only one record per business key has `IsLatest=1`
- Check that LastModified timestamps are consistent with processing order
- Validate business key uniqueness constraints

#### Partition Switch Validation
- Verify staging table structure matches target
- Validate check constraints for partition alignment
- Ensure indexes are properly aligned
- Check partition boundary compliance

### 10. Recovery and Backfill Capabilities
- **Automatic Recovery**: Detect and recover from partial failures for all table types
- **Backfill Support**: 
  - **Partition Tables**: Process historical partition value ranges
  - **SCD Tables**: Process historical date ranges with proper SCD logic
  - **Static Tables**: Force re-extraction and merge regardless of change detection
- **Selective Processing**: 
  - Process specific partition values on demand
  - Process specific date ranges for SCD tables
  - Force refresh specific static tables
- **Cleanup Handling**: Handle scenarios where Parquet files or database data is manually deleted
- **SCD Repair**: Ability to rebuild IsLatest flags based on LastModified timestamps
- **Static Table Repair**: Rebuild static tables from source when inconsistencies detected
- **Partition Switch Recovery**: Rollback failed partition switches and retry

### 11. Implementation Class Structure

#### Main Pipeline Class
```python
class IncrementalSQLServerParquetPipeline:
    def __init__(self, source_connection_string, target_connection_string, parquet_storage_path)
    
    # Core pipeline methods
    def run_daily_incremental_pipeline(partition_tables=None, scd_tables=None, static_tables=None, **kwargs)
    def extract_incremental_to_parquet(table_config, **kwargs)
    def load_incremental_from_parquet(parquet_files, target_schema="dbo")
    
    # Table type detection
    def detect_table_type(table_name)
    def get_table_business_keys(table_name)
    def detect_partition_column(table_name)
    def get_table_primary_keys(table_name)
    def detect_sql_server_partitions(table_name, schema="dbo")
    
    # Partition table methods
    def extract_partition_tables_to_parquet(table_config, target_partition_value=None)
    def get_last_processed_partition_value(table_name, partition_column)
    def get_available_partition_values(table_name, partition_column, start_value=None)
    def load_partitioned_table_from_parquet(parquet_file, table_name, partition_value, table_config, schema="dbo")
    
    # SQL Server partition switching methods
    def determine_load_strategy(table_name, partition_value, table_config)
    def create_staging_table(source_table, partition_value, schema="dbo")
    def switch_partition(source_table, staging_table, partition_value, schema="dbo")
    def validate_partition_switching_prerequisites(table_name, schema="dbo")
    def parallel_partition_load(table_name, partition_values, table_config, max_parallel=4, schema="dbo")
    def get_partition_number(table_name, partition_value, schema="dbo")
    def rebuild_staging_indexes(staging_table, schema="dbo")
    def update_statistics(table_name, schema="dbo")
    
    # SCD table methods  
    def extract_scd_tables_to_parquet(table_config, target_timestamp=None)
    def get_last_processed_timestamp(table_name)
    def load_scd_from_parquet(parquet_files, table_config, target_schema="dbo")
    def update_islatest_flags(table_name, business_keys, target_schema="dbo")
    
    # Static table methods
    def extract_static_tables_to_parquet(table_config, force_extract=False)
    def get_table_content_hash(table_name)
    def get_last_content_hash_from_parquet(table_name)
    def load_static_from_parquet(parquet_files, table_config, target_schema="dbo")
    def merge_static_table(table_name, primary_keys, merge_strategy, target_schema="dbo")
    
    # State detection methods (works for all table types)
    def get_last_processed_partition_value_from_parquet(table_name, partition_column)
    def get_last_processed_partition_value_from_target_db(table_name, partition_column)
    def get_last_processed_timestamp_from_parquet(table_name)
    def get_last_processed_timestamp_from_target_db(table_name)
    def get_last_static_state_from_parquet(table_name)
    def get_static_state_from_target_db(table_name)
    
    # Analysis methods
    def get_missing_data_analysis(table_config)
    def get_pipeline_status(table_config)
    def validate_decimal_precision(tables)
    def validate_scd_integrity(scd_tables)
    def validate_static_integrity(static_tables)
    def validate_partition_alignment(table_name, schema="dbo")
    
    # Utility methods
    def backfill_partition_data(table_config, start_value, end_value)
    def backfill_scd_data(table_config, start_date, end_date)
    def force_refresh_static_tables(table_config)
    def repair_scd_islatest_flags(table_name, business_keys)
    def repair_static_table(table_name, primary_keys, target_schema="dbo")
    def cleanup_staging_tables(pattern="*_STAGE_*", schema="dbo")
```

### 12. Error Handling Requirements
- **Database Connection Errors**: Graceful handling of connection failures
- **Data Type Conversion Errors**: Log and handle conversion failures
- **File System Errors**: Handle Parquet file read/write failures
- **Partial Load Recovery**: Resume from last successful point if pipeline fails mid-process
- **Partition Switch Failures**: Rollback staging table changes and retry
- **Staging Table Conflicts**: Handle concurrent staging table usage

### 13. Performance Requirements
- **Memory Efficiency**: Process large datasets without excessive memory usage
- **Batch Processing**: Handle tables with millions of rows efficiently
- **Arrow Optimization**: Use Arrow's columnar format for optimal performance
- **Parquet Compression**: Leverage Parquet's built-in compression
- **Partition Switch Performance**: Minimize downtime using partition switching
- **Parallel Processing**: Support parallel partition loading for improved throughput

### 14. Dependencies
```
dlt[mssql,parquet]
pyodbc
sqlalchemy
pandas
pyarrow
pathlib (standard library)
logging (standard library)
datetime (standard library)
json (standard library)
decimal (standard library)
concurrent.futures (standard library)
```

### 15. Usage Patterns

#### Daily Operations - All Table Types
```python
# Initialize pipeline
pipeline = IncrementalSQLServerParquetPipeline(source_conn, target_conn, "./parquet_data")

# Configure all table types including SQL Server partitioned tables
table_config = {
    "partition_tables": {
        "large_fact_table": {
            "partition_column": "COBID",
            "initial_load_count": 30,
            "sql_server_partitioned": True,
            "partition_switching": {
                "enabled": True,
                "switch_method": "auto",
                "staging_table_suffix": "_TRANSFORM",
                "validate_schema": True,
                "compress_staging": True,
                "parallel_load": True,
                "max_parallel": 4
            }
        },
        "daily_transactions": {
            "partition_column": "COBID",
            "initial_load_count": 10,
            "sql_server_partitioned": False
        },
        "daily_balances": {
            "partition_column": "ReportingDate",
            "initial_load_count": 5,
            "sql_server_partitioned": True,
            "partition_switching": {
                "enabled": True,
                "switch_method": "always_stage"
            }
        }
    },
    "scd_tables": {
        "customer_dim": {"business_keys": ["customer_id"]},
        "product_dim": {"business_keys": ["product_code", "version"]}
    },
    "static_tables": {
        "currency_codes": {
            "primary_keys": ["currency_code"],
            "merge_strategy": "upsert"
        },
        "country_lookup": {
            "primary_keys": ["country_id"],
            "merge_strategy": "upsert",
            "allow_deletes": False
        }
    }
}

# Check status across all table types
status = pipeline.get_pipeline_status(table_config)

# Run incremental pipeline for all table types
pipeline.run_daily_incremental_pipeline(
    partition_tables=table_config["partition_tables"],
    scd_tables=table_config["scd_tables"],
    static_tables=table_config["static_tables"],
    initial_load_days=30,
    force_partition_switching=False,  # Use config settings
    partition_switch_parallel=True
)

# Validate results across all table types
all_tables = (list(table_config["partition_tables"].keys()) + 
              list(table_config["scd_tables"].keys()) + 
              list(table_config["static_tables"].keys()))
decimal_validation = pipeline.validate_decimal_precision(all_tables)
scd_validation = pipeline.validate_scd_integrity(table_config["scd_tables"])
static_validation = pipeline.validate_static_integrity(table_config["static_tables"])

# Validate partition alignment for SQL Server partitioned tables
for table_name, config in table_config["partition_tables"].items():
    if config.get("sql_server_partitioned", False):
        partition_validation = pipeline.validate_partition_alignment(table_name)
```

#### Recovery Operations
```python
# Analyze missing data across all table types
analysis = pipeline.get_missing_data_analysis(table_config)

# Backfill partition tables with partition switching
pipeline.backfill_partition_data(
    table_config={
        "large_fact_table": {
            "partition_column": "COBID",
            "sql_server_partitioned": True,
            "partition_switching": {
                "enabled": True,
                "switch_method": "auto",
                "parallel_load": True,
                "max_parallel": 4
            }
        }
    },
    start_value=20240101, 
    end_value=20240131
)

# Backfill SCD tables
pipeline.backfill_scd_data(
    table_config={"customer_dim": {"business_keys": ["customer_id"]}},
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Force refresh static tables (ignore change detection)
pipeline.force_refresh_static_tables(
    table_config={
        "currency_codes": {
            "primary_keys": ["currency_code"],
            "merge_strategy": "upsert"
        }
    }
)

# Cleanup failed staging tables
pipeline.cleanup_staging_tables(pattern="*_STAGE_*")
```

#### Partition Switching Operations
```python
# Validate prerequisites for partition switching
for table_name in ["large_fact_table", "daily_balances"]:
    prereqs = pipeline.validate_partition_switching_prerequisites(table_name)
    if not all(prereqs.values()):
        print(f"Warning: {table_name} does not meet all partition switching requirements")
        print(f"Failed checks: {[k for k,v in prereqs.items() if not v]}")

# Process single partition with explicit switching
pipeline.load_partitioned_table_from_parquet(
    parquet_file="./parquet_data/organized/COBID_20240815/large_fact_table_COBID_20240815.parquet",
    table_name="large_fact_table",
    partition_value=20240815,
    table_config={
        "sql_server_partitioned": True,
        "partition_switching": {
            "enabled": True,
            "switch_method": "always_stage",
            "staging_table_suffix": "_TRANSFORM"
        }
    }
)

# Parallel load multiple partitions
partition_values = [20240815, 20240816, 20240817, 20240818]
pipeline.parallel_partition_load(
    table_name="large_fact_table",
    partition_values=partition_values,
    table_config={
        "sql_server_partitioned": True,
        "partition_switching": {
            "enabled": True,
            "switch_method": "auto"
        }
    },
    max_parallel=4
)
```

#### Static Table Operations
```python
# Process only static tables
pipeline.run_daily_incremental_pipeline(
    static_tables={
        "currency_codes": {
            "primary_keys": ["currency_code"],
            "merge_strategy": "upsert"
        },
        "country_lookup": {
            "primary_keys": ["country_id"],
            "merge_strategy": "replace"
        }
    }
)

# Check for changes without extraction
for table_name in ["currency_codes", "country_lookup"]:
    source_hash = pipeline.get_table_content_hash(table_name)
    last_hash = pipeline.get_last_content_hash_from_parquet(table_name)
    
    if source_hash != last_hash:
        print(f"{table_name} has changes - hash mismatch")
    else:
        print(f"{table_name} unchanged since last extraction")

# Validate static table integrity
static_issues = pipeline.validate_static_integrity(["currency_codes", "country_lookup"])
for table, issues in static_issues.items():
    if issues["row_count_mismatch"]:
        print(f"Warning: {table} row count mismatch between source and target")
    if issues["hash_mismatch"]:
        print(f"Warning: {table} content hash mismatch")
```

### 16. Expected Behavior

#### First Run - Partition Tables
- Discover all available partition values in source tables for each configured partition column
- Process only the last N partition values (configurable per table) per table
- Create organized Parquet files using partition column names in paths
- For SQL Server partitioned tables:
  - Detect partition scheme and function
  - Create staging tables for each partition
  - Load data to staging tables
  - Execute partition switches
- For non-partitioned tables:
  - Load data directly to target using append strategy

#### First Run - SCD Tables
- Discover records modified in the last N days (default: 30)
- Process all records in scope
- Create organized Parquet files with timestamp-based naming
- Load data to target database with proper SCD logic
- Ensure IsLatest flags are set correctly

#### First Run - Static Reference Tables
- Calculate content hash of entire source table
- No previous Parquet files exist, so extract complete recordset
- Create Parquet file with content hash and extraction timestamp in metadata
- Load complete recordset to target database using configured merge strategy

#### Subsequent Runs - Partition Tables
- Detect last processed partition value by examining Parquet files and target database for each table's partition column
- Process only new partition values since last successful run
- For SQL Server partitioned tables:
  - Determine load strategy (direct vs. partition switch)
  - If switching: create staging table, load data, validate, switch partition
  - If direct: append new data to target tables
- For non-partitioned tables:
  - Append new data to target tables

#### Subsequent Runs - SCD Tables
- Detect last processed LastModified timestamp
- Extract records with LastModified > last processed timestamp
- Process SCD logic:
  1. Identify records with existing business keys
  2. Set IsLatest=0 for superseded records
  3. Insert new records with IsLatest=1
  4. Insert entirely new business keys with IsLatest=1

#### Subsequent Runs - Static Reference Tables
- Calculate current content hash of source table
- Compare with hash from last Parquet file
- If hashes match: Skip extraction (no changes detected)
- If hashes differ: Extract complete recordset and create new Parquet file
- Merge changes into target database using configured strategy (upsert/replace)
- Update Parquet metadata with new hash and timestamp

#### Recovery Scenarios - Universal
- **Parquet files deleted**: Recreate from target database state
- **Target database data lost**: Reload from existing Parquet files
- **Partial failures**: Resume from last consistent state
- **Manual intervention**: Self-correct based on actual data state

#### Recovery Scenarios - Partition Switching
- **Failed partition switch**: Rollback staging table changes, cleanup, retry
- **Orphaned staging tables**: Detect and cleanup abandoned staging tables
- **Partition misalignment**: Detect and report partition boundary issues
- **Index corruption**: Rebuild indexes on staging tables before retry

#### SCD-Specific Recovery
- **IsLatest flag corruption**: Rebuild flags based on LastModified timestamps per business key
- **Duplicate latest records**: Identify and resolve multiple IsLatest=1 records for same business key
- **Missing historical versions**: Detect gaps in version history

### 17. Quality Assurance
- **Data Integrity**: Ensure no data loss or duplication for all table types
- **Precision Preservation**: Maintain exact decimal precision
- **Consistency Validation**: Verify data consistency across pipeline stages
- **Performance Monitoring**: Track processing times and resource usage
- **SCD Integrity**: Ensure proper SCD Type 2 semantics are maintained
- **Business Rule Compliance**: Validate that business logic constraints are met
- **Partition Switch Validation**: Verify successful partition switches with no data loss
- **Automated Testing**: Support for automated validation of pipeline results

### 18. Partition Switching Implementation Details

#### Staging Table Creation
```sql
-- Example staging table creation with proper constraints
CREATE TABLE [dbo].[large_fact_table_STAGE_20240815]
WITH (DATA_COMPRESSION = PAGE)
AS SELECT TOP 0 * FROM [dbo].[large_fact_table];

-- Add check constraint for partition alignment
ALTER TABLE [dbo].[large_fact_table_STAGE_20240815]
ADD CONSTRAINT CK_large_fact_table_STAGE_20240815_COBID 
CHECK (COBID = 20240815);

-- Create aligned indexes
CREATE CLUSTERED INDEX IX_large_fact_table_STAGE_20240815
ON [dbo].[large_fact_table_STAGE_20240815] (COBID, [other_key_columns])
WITH (DATA_COMPRESSION = PAGE);
```

#### Partition Switch Execution
```sql
-- Switch out old partition (if exists)
ALTER TABLE [dbo].[large_fact_table] 
SWITCH PARTITION $PARTITION.partition_function(20240815) 
TO [dbo].[large_fact_table_ARCHIVE_20240815];

-- Switch in new data
ALTER TABLE [dbo].[large_fact_table_STAGE_20240815] 
SWITCH TO [dbo].[large_fact_table] 
PARTITION $PARTITION.partition_function(20240815);
```

#### Validation Queries
```sql
-- Verify partition alignment
SELECT 
    p.partition_number,
    p.rows,
    prv.value as boundary_value
FROM sys.partitions p
JOIN sys.tables t ON p.object_id = t.object_id
LEFT JOIN sys.partition_range_values prv 
    ON prv.function_id = (
        SELECT pf.function_id 
        FROM sys.partition_functions pf
        JOIN sys.partition_schemes ps ON pf.function_id = ps.function_id
        JOIN sys.indexes i ON ps.data_space_id = i.data_space_id
        WHERE i.object_id = t.object_id AND i.index_id <= 1
    )
    AND p.partition_number = prv.boundary_id + 1
WHERE t.name = 'large_fact_table'
ORDER BY p.partition_number;

-- Verify staging table meets switch requirements
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'No check constraint found'
        WHEN COUNT(*) > 1 THEN 'Multiple check constraints found'
        ELSE 'Check constraint valid'
    END as constraint_status
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID('[dbo].[large_fact_table_STAGE_20240815]')
    AND definition LIKE '%COBID%=%20240815%';
```

### 19. Static Reference Table Implementation Requirements

#### Change Detection Strategy
- **Content Hashing**: Use SQL Server `CHECKSUM_AGG` or similar to calculate table-level hash
- **Row-Level Hashing**: Optional support for row-level change detection using `HASHBYTES`
- **Metadata Comparison**: Compare row counts, last modified timestamps if available
- **Efficient Detection**: Minimize overhead for tables that haven't changed

#### Hash Calculation Methods
- **Simple Hash**: `SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM table_name ORDER BY primary_key`
- **Binary Hash**: `SELECT HASHBYTES('SHA2_256', (SELECT * FROM table_name ORDER BY primary_key FOR XML RAW))`
- **Custom Hash**: Support for custom hash calculations based on specific columns

#### Parquet Metadata Management
- **Hash Storage**: Store content hash in Parquet file metadata
- **Timestamp Storage**: Store extraction timestamp for audit trails
- **Row Count Storage**: Store row count for validation
- **Schema Version**: Track schema changes that might affect hashing

#### Merge Strategy Implementation
- **Upsert Strategy**: 
  1. Compare source and target using primary keys
  2. INSERT new records
  3. UPDATE changed records
  4. Optionally DELETE missing records
- **Replace Strategy**: 
  1. TRUNCATE target table
  2. INSERT complete source recordset
- **Performance**: Use MERGE statements or bulk operations for efficiency

#### Static Table Validation
- **Row Count Validation**: Verify source and target row counts match
- **Hash Validation**: Ensure target table produces same hash as source
- **Primary Key Validation**: Verify no duplicate or null primary keys
- **Schema Validation**: Ensure target table schema matches source

#### Optimization for Small Tables
- **Memory Processing**: Load entire table into memory for small tables (<10MB)
- **Single Transaction**: Process entire table in single transaction for consistency
- **Minimal Logging**: Use appropriate SQL Server bulk load options
- **Index Management**: Temporarily disable non-clustered indexes during large updates

### 20. SCD Type 2 Implementation Requirements

#### Business Key Management
- **Automatic Detection**: Attempt to detect business keys from table constraints/indexes
- **Manual Configuration**: Allow explicit configuration of business key columns
- **Composite Keys**: Support multiple-column business keys
- **Key Validation**: Validate that business keys are not null in source data

#### IsLatest Flag Management
- **Update Logic**: When inserting new version of existing business key:
  1. Set `IsLatest=0` for current latest record
  2. Insert new record with `IsLatest=1`
- **Atomicity**: Ensure IsLatest updates and inserts happen atomically
- **Validation**: Verify only one record per business key has `IsLatest=1` after processing
- **Repair Capability**: Rebuild IsLatest flags when inconsistencies are detected

#### LastModified Timestamp Handling
- **Increment Detection**: Use `WHERE LastModified > {last_processed_timestamp}`
- **Timezone Handling**: Properly handle timezone-aware timestamps
- **Precision**: Support timestamp precision to milliseconds/microseconds
- **Boundary Handling**: Handle edge cases where multiple records have identical timestamps

#### SCD Transaction Processing
- **Batch Processing**: Process SCD updates in batches for performance
- **Rollback Capability**: Ability to rollback SCD changes if validation fails
- **Conflict Resolution**: Handle cases where source data has inconsistent LastModified values
- **Performance**: Optimize queries to minimize impact on target database during IsLatest updates

#### SCD-Specific Validation
- **Uniqueness**: Verify IsLatest=1 uniqueness per business key
- **Temporal Consistency**: Ensure LastModified timestamps align with processing order
- **Completeness**: Verify all source changes are reflected in target
- **Performance Monitoring**: Track SCD processing times and affected row counts

### 21. Advanced Configuration Examples

#### Mixed Environment Configuration (PROD to UAT)
```python
# Configuration for PROD to UAT sync with reduced data retention
prod_to_uat_config = {
    "source_connection": "prod_sql_server_connection_string",
    "target_connection": "uat_sql_server_connection_string",
    "partition_tables": {
        # Large partitioned fact table - use partition switching
        "fact_daily_transactions": {
            "partition_column": "COBID",
            "initial_load_count": 30,  # Only last 30 days for UAT
            "sql_server_partitioned": True,
            "partition_switching": {
                "enabled": True,
                "switch_method": "auto",
                "staging_table_suffix": "_TRANSFORM",
                "validate_schema": True,
                "compress_staging": True,
                "parallel_load": True,
                "max_parallel": 4,
                "cleanup_archives": True,  # Don't keep archives in UAT
                "archive_retention_days": 0
            }
        },
        # Medium-sized table - use partition switching
        "fact_risk_metrics": {
            "partition_column": "BusinessDate",
            "initial_load_count": 15,
            "sql_server_partitioned": True,
            "partition_switching": {
                "enabled": True,
                "switch_method": "always_stage",
                "staging_table_suffix": "_STAGE",
                "parallel_load": False
            }
        },
        # Small table - direct load
        "dim_date": {
            "partition_column": "DateKey",
            "initial_load_count": 365,
            "sql_server_partitioned": False
        }
    },
    "scd_tables": {
        "dim_customer": {
            "business_keys": ["customer_id"],
            "initial_load_days": 30,
            "batch_size": 50000
        },
        "dim_product": {
            "business_keys": ["product_code", "product_version"],
            "initial_load_days": 7,
            "batch_size": 10000
        }
    },
    "static_tables": {
        "ref_currency": {
            "primary_keys": ["currency_code"],
            "merge_strategy": "upsert",
            "change_detection_method": "checksum"
        },
        "ref_country": {
            "primary_keys": ["country_id"],
            "merge_strategy": "replace",
            "change_detection_method": "hash"
        }
    },
    "global_settings": {
        "parquet_compression": "snappy",
        "decimal_precision_validation": True,
        "error_threshold": 0.01,  # Allow 1% error rate
        "enable_monitoring": True,
        "log_level": "INFO"
    }
}
```

#### Airflow DAG Integration
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def run_partition_tables(**context):
    pipeline = IncrementalSQLServerParquetPipeline(
        source_connection_string=context['params']['source_conn'],
        target_connection_string=context['params']['target_conn'],
        parquet_storage_path=context['params']['parquet_path']
    )
    
    # Check for partition switching prerequisites
    for table_name, config in context['params']['partition_tables'].items():
        if config.get('sql_server_partitioned', False):
            prereqs = pipeline.validate_partition_switching_prerequisites(table_name)
            if not all(prereqs.values()):
                raise Exception(f"Partition switching prerequisites not met for {table_name}")
    
    # Run partition table pipeline
    pipeline.run_daily_incremental_pipeline(
        partition_tables=context['params']['partition_tables'],
        force_partition_switching=context['params'].get('force_switching', False)
    )
    
    # Return status for XCom
    return pipeline.get_pipeline_status({"partition_tables": context['params']['partition_tables']})

def run_scd_tables(**context):
    pipeline = IncrementalSQLServerParquetPipeline(
        source_connection_string=context['params']['source_conn'],
        target_connection_string=context['params']['target_conn'],
        parquet_storage_path=context['params']['parquet_path']
    )
    
    pipeline.run_daily_incremental_pipeline(
        scd_tables=context['params']['scd_tables']
    )
    
    # Validate SCD integrity
    validation_results = pipeline.validate_scd_integrity(context['params']['scd_tables'])
    if any(issues for issues in validation_results.values()):
        raise Exception(f"SCD integrity validation failed: {validation_results}")
    
    return pipeline.get_pipeline_status({"scd_tables": context['params']['scd_tables']})

def run_static_tables(**context):
    pipeline = IncrementalSQLServerParquetPipeline(
        source_connection_string=context['params']['source_conn'],
        target_connection_string=context['params']['target_conn'],
        parquet_storage_path=context['params']['parquet_path']
    )
    
    pipeline.run_daily_incremental_pipeline(
        static_tables=context['params']['static_tables']
    )
    
    return pipeline.get_pipeline_status({"static_tables": context['params']['static_tables']})

# Define DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'prod_to_uat_sync',
    default_args=default_args,
    description='Sync PROD data to UAT environment',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    params=prod_to_uat_config
)

# Define tasks
partition_task = PythonOperator(
    task_id='sync_partition_tables',
    python_callable=run_partition_tables,
    provide_context=True,
    dag=dag
)

scd_task = PythonOperator(
    task_id='sync_scd_tables',
    python_callable=run_scd_tables,
    provide_context=True,
    dag=dag
)

static_task = PythonOperator(
    task_id='sync_static_tables',
    python_callable=run_static_tables,
    provide_context=True,
    dag=dag
)

# Set dependencies - static tables first, then dimensions, then facts
static_task >> scd_task >> partition_task
```

### 22. Performance Benchmarks and Expectations

#### Partition Switching Performance
- **Switch Operation**: < 1 second per partition (metadata operation only)
- **Staging Table Load**: Dependent on data volume, typically 100K-1M rows/minute
- **Index Rebuild**: 10-60 seconds depending on table size
- **Total Time per Partition**: Load time + 1-2 minutes overhead

#### Direct Load Performance
- **Small Tables** (< 100K rows): 5-30 seconds
- **Medium Tables** (100K-1M rows): 1-5 minutes
- **Large Tables** (> 1M rows): Consider partition switching

#### Parallel Processing Gains
- **2 Parallel Partitions**: ~1.8x speedup
- **4 Parallel Partitions**: ~3.5x speedup
- **8 Parallel Partitions**: ~6x speedup (diminishing returns)

### 23. Troubleshooting Guide

#### Common Partition Switching Issues
1. **"Table is not partitioned"**: Target table doesn't have SQL Server partitioning
   - Solution: Set `sql_server_partitioned: False` in config
   
2. **"Check constraint violation"**: Staging table constraint doesn't match partition
   - Solution: Verify partition_column value matches data
   
3. **"Non-aligned index"**: Indexes not partition-aligned
   - Solution: Rebuild indexes with partition scheme
   
4. **"Filegroup not available"**: Partition's filegroup is offline
   - Solution: Bring filegroup online or use different partition

#### Recovery Procedures
1. **Failed Partition Switch**:
   ```python
   # Cleanup and retry
   pipeline.cleanup_staging_tables(pattern=f"*_STAGE_{partition_value}")
   pipeline.load_partitioned_table_from_parquet(
       parquet_file, table_name, partition_value, 
       table_config, schema="dbo"
   )
   ```

2. **Orphaned Staging Tables**:
   ```python
   # Find and cleanup orphaned tables
   orphaned = pipeline.find_orphaned_staging_tables()
   for table in orphaned:
       pipeline.drop_table(table)
   ```

3. **Partition Boundary Issues**:
   ```python
   # Validate and report boundary mismatches
   issues = pipeline.validate_partition_alignment(table_name)
   if issues['boundary_mismatches']:
       pipeline.report_partition_boundary_issues(table_name)
   ```

### 24. Monitoring and Alerting

#### Key Metrics to Monitor
- **Partition Switch Success Rate**: Target > 99%
- **Average Switch Duration**: Baseline and track deviations
- **Staging Table Space Usage**: Monitor for cleanup failures
- **Parallel Load Efficiency**: CPU and I/O utilization
- **Data Freshness**: Gap between source and target partition values

#### Alert Conditions
- **Critical**:
  - Partition switch failure after retries
  - Data corruption detected post-switch
  - No successful loads in 24 hours
  
- **Warning**:
  - Partition switch taking > 5x normal duration
  - Staging table space > threshold
  - Parallel load efficiency < 50%
  
- **Info**:
  - New partition boundary created
  - Manual intervention performed
  - Configuration change detected

This comprehensive requirements document now fully incorporates SQL Server native partition switching support, providing optimal performance for large table synchronization between PROD and UAT environments while maintaining data integrity and supporting various table patterns.
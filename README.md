# SQL Server Incremental Data Pipeline

A comprehensive, enterprise-ready data pipeline for incremental data extraction from SQL Server using the [dlt (data load tool)](https://dlthub.com/) library. This pipeline supports three distinct table types with advanced features like SQL Server partition switching, SCD Type 2 history tracking, and hash-based change detection.

## 🌟 Features

### Core Capabilities
- **Three Table Types**: Partition tables, SCD Type 2 tables, and static reference tables
- **SQL Server Partition Switching**: Optimized bulk data movement using SQL Server native features
- **Self-Healing State Management**: Automatic recovery from failures using Parquet metadata and database state
- **High-Precision Decimal Handling**: Native Arrow decimal128 support for financial data
- **Parallel Processing**: Configurable parallel execution for optimal performance
- **Comprehensive Validation**: End-to-end data validation and integrity checks

### Advanced Features
- **Change Detection**: Hash-based change detection for static tables
- **SCD Type 2 Logic**: Full slowly changing dimension support with history tracking
- **State Recovery**: Conservative recovery approach using minimum values for safety
- **Partition Optimization**: SQL Server partition function and scheme integration
- **Error Handling**: Robust error handling with configurable retry logic
- **Structured Logging**: Context-aware logging with performance metrics

## 📋 Requirements

### System Requirements
- Python 3.9+
- SQL Server 2016+ (for partition switching features)
- ODBC Driver 18 for SQL Server
- 4GB+ RAM (recommended for large datasets)

### Python Dependencies
- `dlt[mssql,parquet]>=0.4.12`
- `pyarrow>=14.0.1`
- `pandas>=2.1.4`
- `sqlalchemy>=2.0.23`
- `pydantic>=2.5.0`
- `structlog>=23.2.0`

## 🚀 Quick Start

### 1. Installation

```bash
git clone <repository-url>
cd dlt-sync
pip install -r requirements.txt
pip install -e .
```

### 2. Basic Configuration

```python
from src.config.models import (
    PipelineConfig, DatabaseConfig, 
    PartitionTableConfig, SCDTableConfig, StaticTableConfig
)

# Database configuration
database_config = DatabaseConfig(
    server="your-sql-server",
    database="your-database",
    integrated_security=True,  # or use username/password
    trust_server_certificate=True
)

# Configure tables
config = PipelineConfig(
    database=database_config,
    partition_tables=[
        PartitionTableConfig(
            table_name="Orders",
            schema_name="sales",
            target_table="orders",
            target_schema="warehouse",
            partition_column="OrderDate"
        )
    ],
    scd_tables=[
        SCDTableConfig(
            table_name="Customers",
            schema_name="crm",
            target_table="customers",
            target_schema="warehouse",
            business_key_columns=["CustomerID"],
            last_modified_column="LastModified",
            is_latest_column="IsLatest"
        )
    ],
    static_tables=[
        StaticTableConfig(
            table_name="Products",
            schema_name="catalog",
            target_table="products",
            target_schema="warehouse",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace"
        )
    ],
    pipeline_name="my_pipeline"
)
```

### 3. Run the Pipeline

```python
from src.pipeline.core import IncrementalSQLServerParquetPipeline

# Initialize pipeline
pipeline = IncrementalSQLServerParquetPipeline(config)

# Run all tables
results = pipeline.run(parallel_execution=True)

print(f"Processed {results['tables_processed']} tables")
print(f"Total records: {results['total_records']:,}")
print(f"Duration: {results['total_duration_seconds']:.2f} seconds")
```

## 📊 Table Types

### 1. Partition Tables
Best for large transaction tables partitioned by date or other columns.

**Features:**
- SQL Server partition switching for optimal performance
- Parallel partition processing
- Automatic staging table management
- Support for custom partition functions and schemes

**Use Cases:**
- Sales transactions
- Log data
- Time-series data
- Large fact tables

### 2. SCD Type 2 Tables  
Perfect for master data requiring history tracking.

**Features:**
- Automatic business key detection
- Timestamp-based change identification
- IsLatest flag management
- Effective/end date handling
- Data integrity validation and repair

**Use Cases:**
- Customer master data
- Product catalogs
- Employee records  
- Any slowly changing dimensions

### 3. Static Reference Tables
Ideal for reference data with infrequent changes.

**Features:**
- Hash-based change detection
- Multiple hash algorithms (CHECKSUM, MD5, SHA256)
- Replace or upsert strategies
- Force reload based on age
- Data quality validation

**Use Cases:**
- Country/region codes
- Product categories
- Configuration tables
- Lookup tables

## ⚙️ Configuration

### Database Configuration
```python
DatabaseConfig(
    server="sql-server.company.com",
    database="DataWarehouse",
    username="etl_user",           # Optional with integrated auth
    password="secure_password",    # Optional with integrated auth
    port=1433,
    integrated_security=False,     # Use SQL Server auth
    trust_server_certificate=True,
    connection_timeout=30,
    command_timeout=600
)
```

### Processing Configuration
```python
ProcessingConfig(
    batch_size=50000,                          # Records per batch
    max_workers=8,                             # Parallel workers
    memory_limit_mb=2048,                      # Memory per worker
    retry_attempts=3,                          # Retry count
    partition_parallel_limit=4,                # Partition parallelism
    enable_sql_server_partition_switching=True,
    validate_precision_end_to_end=True
)
```

### State Management
```python
StateConfig(
    state_directory=Path("./state"),
    parquet_directory=Path("./data"),
    enable_self_healing=True,
    state_retention_days=30,
    validate_state_consistency=True,
    backup_state_before_run=True
)
```

## 🔧 Advanced Usage

### Running Specific Table Types
```python
# Run only partition tables
results = pipeline.run(table_types=["partition"])

# Run only SCD tables  
results = pipeline.run(table_types=["scd"])

# Run specific tables by name
results = pipeline.run(table_names=["sales.Orders", "crm.Customers"])
```

### Validation and Monitoring
```python
# Validate all table configurations
validation = pipeline.validate_all_tables()
print(f"Valid tables: {validation['valid_tables']}/{validation['total_tables']}")

# Get pipeline status
status = pipeline.get_pipeline_status()
print(f"Database connected: {status['database_connected']}")
```

### SCD Integrity Repair
```python
# Repair SCD integrity issues
repair_results = pipeline.scd_handler.repair_scd_integrity(
    scd_table_config,
    dry_run=False  # Set to True to preview changes
)
```

## 🐋 Docker Deployment

### Dockerfile
```dockerfile
FROM python:3.11-slim

# Install ODBC drivers
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev

# Install Microsoft ODBC Driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY . /app
WORKDIR /app
RUN pip install -e .

CMD ["python", "examples/basic_usage.py"]
```

### Docker Compose
```yaml
version: '3.8'
services:
  dlt-sync:
    build: .
    environment:
      - DLT_SYNC_SERVER=sql-server
      - DLT_SYNC_DATABASE=DataWarehouse
      - DLT_SYNC_USERNAME=etl_user
      - DLT_SYNC_PASSWORD=secure_password
    volumes:
      - ./data:/app/data
      - ./state:/app/state
      - ./logs:/app/logs
    depends_on:
      - sql-server
```

## 🚁 Apache Airflow Integration

The pipeline includes a comprehensive Airflow DAG example with:

- **Task Groups**: Organized by table type
- **Validation Tasks**: Pre-execution validation
- **Parallel Processing**: Optimized task dependencies  
- **Error Handling**: Robust failure management
- **Monitoring**: XCom integration for results tracking

```python
# See examples/airflow_dag_example.py for complete implementation
from examples.airflow_dag_example import dag

# Key features:
# - Daily schedule at 2 AM
# - Validation before processing
# - Parallel table type processing
# - Comprehensive error handling
# - Cleanup tasks
```

## 📈 Performance Optimization

### SQL Server Partition Switching
For optimal performance with large partitioned tables:

1. **Enable partition switching** in configuration
2. **Align partition schemes** between source and target
3. **Use proper constraints** for partition elimination
4. **Monitor staging table cleanup**

### Batch Size Tuning
- **Small tables (< 1M rows)**: 10,000 - 50,000
- **Medium tables (1M - 10M rows)**: 50,000 - 200,000  
- **Large tables (> 10M rows)**: 200,000 - 1,000,000

### Memory Management
- **Development**: 512MB per worker
- **Production**: 2GB+ per worker
- **High-volume**: 4GB+ per worker

## 🧪 Testing

### Unit Tests
```bash
pytest tests/ -v
```

### Integration Tests  
```bash
# Requires test database
pytest tests/integration/ -v --db-url="mssql://server/testdb"
```

### Performance Tests
```bash
pytest tests/performance/ -v --benchmark-only
```

## 🔍 Monitoring and Troubleshooting

### Structured Logging
```python
# Configure structured logging
PipelineConfig(
    log_level="INFO",
    enable_structured_logging=True,
    log_file=Path("/var/log/dlt-sync/pipeline.log")
)
```

### Common Issues

**Connection Timeouts**
- Increase `connection_timeout` and `command_timeout`
- Check network connectivity and SQL Server configuration

**Memory Issues**
- Reduce `batch_size` 
- Lower `max_workers`
- Increase `memory_limit_mb`

**Partition Switch Failures**
- Verify table schemas match exactly
- Check constraint compatibility
- Ensure proper permissions

**SCD Integrity Issues**
- Use built-in repair functionality
- Validate business key uniqueness
- Check timestamp column data quality

## 📁 Project Structure

```
dlt-sync/
├── src/
│   ├── config/          # Configuration models
│   ├── pipeline/        # Core pipeline components
│   │   ├── core.py      # Main pipeline class
│   │   ├── *_handler.py # Table type handlers
│   │   └── utils/       # SQL utilities
│   └── utils/           # Logging and validation
├── examples/            # Usage examples
├── tests/              # Test suite
├── requirements.txt    # Dependencies
└── README.md          # This file
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit a pull request

### Development Setup
```bash
git clone <repository-url>
cd dlt-sync
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
pip install -e .
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the [troubleshooting guide](#-monitoring-and-troubleshooting)
2. Review [configuration examples](examples/configuration_examples.py)
3. Open an issue with detailed error information

## 🏗️ Roadmap

- [ ] Support for additional databases (PostgreSQL, Oracle)
- [ ] Real-time change data capture (CDC) integration
- [ ] Advanced data quality rules and validation
- [ ] Cloud-native deployment templates (Kubernetes, Docker Swarm)
- [ ] Integration with data catalogs (Apache Atlas, DataHub)
- [ ] Automated performance tuning recommendations

---

**Built with ❤️ for enterprise data engineering teams**
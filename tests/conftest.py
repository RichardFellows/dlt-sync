"""Test configuration and fixtures."""

import pytest
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock
from datetime import datetime

from src.config.models import (
    DatabaseConfig, ProcessingConfig, StateConfig,
    PartitionTableConfig, SCDTableConfig, StaticTableConfig,
    PipelineConfig
)


@pytest.fixture
def temp_directory():
    """Create a temporary directory for tests."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def database_config():
    """Create a test database configuration."""
    return DatabaseConfig(
        server="test-server",
        database="TestDB",
        integrated_security=True,
        trust_server_certificate=True,
        connection_timeout=15,
        command_timeout=30
    )


@pytest.fixture
def processing_config():
    """Create a test processing configuration."""
    return ProcessingConfig(
        batch_size=1000,
        max_workers=2,
        retry_attempts=2,
        enable_sql_server_partition_switching=True
    )


@pytest.fixture
def state_config(temp_directory):
    """Create a test state configuration."""
    return StateConfig(
        state_directory=temp_directory / "state",
        parquet_directory=temp_directory / "data",
        enable_self_healing=True,
        state_retention_days=7
    )


@pytest.fixture
def partition_table_config():
    """Create a test partition table configuration."""
    return PartitionTableConfig(
        table_name="TestPartitionTable",
        schema_name="test",
        target_table="test_partition_table",
        target_schema="target",
        partition_column="DateColumn",
        batch_size=5000
    )


@pytest.fixture
def scd_table_config():
    """Create a test SCD table configuration."""
    return SCDTableConfig(
        table_name="TestSCDTable",
        schema_name="test",
        target_table="test_scd_table",
        target_schema="target",
        business_key_columns=["ID", "Code"],
        last_modified_column="LastModified",
        is_latest_column="IsLatest",
        effective_date_column="EffectiveDate",
        end_date_column="EndDate",
        batch_size=2000
    )


@pytest.fixture  
def static_table_config():
    """Create a test static table configuration."""
    return StaticTableConfig(
        table_name="TestStaticTable",
        schema_name="test",
        target_table="test_static_table", 
        target_schema="target",
        hash_algorithm="hashbytes_sha256",
        load_strategy="replace",
        force_reload_after_days=7
    )


@pytest.fixture
def pipeline_config(database_config, processing_config, state_config, 
                   partition_table_config, scd_table_config, static_table_config):
    """Create a test pipeline configuration."""
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=[partition_table_config],
        scd_tables=[scd_table_config],
        static_tables=[static_table_config],
        pipeline_name="test_pipeline",
        log_level="DEBUG"
    )


@pytest.fixture
def mock_sql_utils():
    """Create a mock SQL utilities object."""
    mock = Mock()
    
    # Configure default return values
    mock.test_connection.return_value = True
    mock.get_table_schema.return_value = {
        "ID": {
            "data_type": "int",
            "is_nullable": False,
            "precision": None,
            "scale": None
        },
        "Name": {
            "data_type": "varchar", 
            "is_nullable": True,
            "max_length": 100,
            "precision": None,
            "scale": None
        },
        "Amount": {
            "data_type": "decimal",
            "is_nullable": True,
            "precision": 18,
            "scale": 4
        }
    }
    
    mock.get_partition_info.return_value = {
        "is_partitioned": True,
        "partition_function": "TestPartitionFunction",
        "partition_scheme": "TestPartitionScheme", 
        "partition_column": "DateColumn",
        "partitions": [
            {"partition_number": 1, "row_count": 1000, "boundary_value": "2023-01-01"},
            {"partition_number": 2, "row_count": 1500, "boundary_value": "2023-02-01"},
            {"partition_number": 3, "row_count": 2000, "boundary_value": "2023-03-01"}
        ]
    }
    
    mock.get_primary_key_columns.return_value = ["ID"]
    mock.get_table_row_count.return_value = 10000
    mock.get_table_hash.return_value = "abc123def456"
    mock.get_min_max_timestamp.return_value = (
        datetime(2023, 1, 1),
        datetime(2023, 3, 31)
    )
    mock.get_distinct_partition_values.return_value = [
        "2023-01-01", "2023-02-01", "2023-03-01"
    ]
    
    # Partition switching operations
    mock.check_partition_switch_prerequisites.return_value = {
        "source_partitioned": True,
        "target_partitioned": True,
        "compatible_schemas": True,
        "constraints_match": True,
        "indexes_match": True,
        "can_switch": True
    }
    
    mock.create_staging_table_like.return_value = True
    mock.execute_partition_switch.return_value = True
    mock.cleanup_staging_tables.return_value = 2
    
    return mock


@pytest.fixture
def mock_decimal_handler():
    """Create a mock decimal handler."""
    mock = Mock()
    
    # Mock type mappings
    mock.sql_type_to_arrow_type.side_effect = lambda sql_type, precision=None, scale=None: {
        "int": "int32",
        "varchar": "string", 
        "decimal": f"decimal128({precision},{scale})" if precision else "decimal128(18,4)"
    }.get(sql_type, f"unknown({sql_type})")
    
    mock.create_arrow_schema_from_sql_schema.return_value = "mock_arrow_schema"
    mock.validate_decimal_precision_in_table.return_value = []
    mock.format_decimal_for_sql_server.return_value = "123.45"
    
    return mock


@pytest.fixture
def sample_table_data():
    """Create sample table data for testing."""
    return [
        {"ID": 1, "Name": "John", "Amount": 100.50, "DateColumn": "2023-01-01"},
        {"ID": 2, "Name": "Jane", "Amount": 200.75, "DateColumn": "2023-01-02"},
        {"ID": 3, "Name": "Bob", "Amount": 300.25, "DateColumn": "2023-01-03"},
        {"ID": 4, "Name": "Alice", "Amount": 400.00, "DateColumn": "2023-01-04"},
        {"ID": 5, "Name": "Charlie", "Amount": 500.30, "DateColumn": "2023-01-05"}
    ]


@pytest.fixture
def sample_scd_data():
    """Create sample SCD data for testing."""
    base_time = datetime(2023, 1, 1)
    return [
        {
            "ID": 1,
            "Code": "A001", 
            "Name": "Product A",
            "LastModified": base_time,
            "IsLatest": True,
            "EffectiveDate": base_time,
            "EndDate": None
        },
        {
            "ID": 2,
            "Code": "B001",
            "Name": "Product B", 
            "LastModified": base_time,
            "IsLatest": False,
            "EffectiveDate": base_time,
            "EndDate": datetime(2023, 1, 15)
        },
        {
            "ID": 2,
            "Code": "B001", 
            "Name": "Product B Updated",
            "LastModified": datetime(2023, 1, 15),
            "IsLatest": True,
            "EffectiveDate": datetime(2023, 1, 15),
            "EndDate": None
        }
    ]


# Test markers for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", 
        "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration test (requires database)"
    )
    config.addinivalue_line(
        "markers", 
        "slow: mark test as slow running"
    )


# Skip integration tests by default unless --integration flag is provided
def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    if not config.getoption("--integration"):
        skip_integration = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


def pytest_addoption(parser):
    """Add command line options."""
    parser.addoption(
        "--integration", 
        action="store_true", 
        default=False,
        help="run integration tests that require database connectivity"
    )
    
    parser.addoption(
        "--db-url",
        action="store",
        default=None,
        help="database URL for integration tests"
    )
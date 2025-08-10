"""Tests for configuration models."""

import pytest
from pathlib import Path
from src.config.models import (
    DatabaseConfig, ProcessingConfig, StateConfig,
    PartitionTableConfig, SCDTableConfig, StaticTableConfig,
    PipelineConfig
)


class TestDatabaseConfig:
    """Test database configuration."""
    
    def test_valid_config(self):
        """Test valid database configuration."""
        config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            integrated_security=True,
            trust_server_certificate=True
        )
        
        assert config.server == "localhost"
        assert config.database == "TestDB"
        assert config.port == 1433
        
    def test_connection_string_integrated_auth(self):
        """Test connection string with integrated authentication."""
        config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            integrated_security=True,
            trust_server_certificate=True
        )
        
        conn_str = config.get_connection_string()
        assert "Trusted_Connection=yes" in conn_str
        assert "SERVER=localhost,1433" in conn_str
        assert "DATABASE=TestDB" in conn_str
        
    def test_connection_string_sql_auth(self):
        """Test connection string with SQL authentication."""
        config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            username="testuser",
            password="testpass",
            integrated_security=False,
            trust_server_certificate=True
        )
        
        conn_str = config.get_connection_string()
        assert "UID=testuser" in conn_str
        assert "PWD=testpass" in conn_str
        assert "Trusted_Connection" not in conn_str
        
    def test_invalid_port(self):
        """Test invalid port validation."""
        with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
            DatabaseConfig(
                server="localhost",
                database="TestDB",
                port=70000,
                integrated_security=True
            )
    
    def test_missing_credentials_sql_auth(self):
        """Test missing credentials for SQL authentication."""
        with pytest.raises(ValueError, match="Username and password required"):
            DatabaseConfig(
                server="localhost",
                database="TestDB",
                integrated_security=False
            )


class TestProcessingConfig:
    """Test processing configuration."""
    
    def test_default_values(self):
        """Test default processing configuration."""
        config = ProcessingConfig()
        
        assert config.batch_size == 10000
        assert config.max_workers == 4
        assert config.retry_attempts == 3
        assert config.enable_sql_server_partition_switching == True
        
    def test_custom_values(self):
        """Test custom processing configuration."""
        config = ProcessingConfig(
            batch_size=50000,
            max_workers=8,
            retry_attempts=5
        )
        
        assert config.batch_size == 50000
        assert config.max_workers == 8
        assert config.retry_attempts == 5
        
    def test_invalid_values(self):
        """Test validation of invalid values."""
        with pytest.raises(ValueError, match="Value must be positive"):
            ProcessingConfig(batch_size=0)
            
        with pytest.raises(ValueError, match="Value must be positive"):
            ProcessingConfig(max_workers=-1)


class TestTableConfigs:
    """Test table configuration models."""
    
    def test_partition_table_config(self):
        """Test partition table configuration."""
        config = PartitionTableConfig(
            table_name="TestTable",
            target_table="test_target",
            partition_column="DateCol"
        )
        
        assert config.table_name == "TestTable"
        assert config.target_table == "test_target"
        assert config.partition_column == "DateCol"
        assert config.schema_name == "dbo"  # default
        
    def test_scd_table_config(self):
        """Test SCD table configuration."""
        config = SCDTableConfig(
            table_name="TestSCD",
            target_table="test_scd_target",
            business_key_columns=["ID", "Code"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsLatest"
        )
        
        assert config.table_name == "TestSCD"
        assert config.business_key_columns == ["ID", "Code"]
        assert config.last_modified_column == "ModifiedDate"
        
    def test_scd_table_config_validation(self):
        """Test SCD table configuration validation."""
        with pytest.raises(ValueError, match="At least one business key column is required"):
            SCDTableConfig(
                table_name="TestSCD",
                target_table="test_target",
                business_key_columns=[],
                last_modified_column="ModifiedDate",
                is_latest_column="IsLatest"
            )
            
    def test_static_table_config(self):
        """Test static table configuration."""
        config = StaticTableConfig(
            table_name="TestStatic",
            target_table="test_static_target",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace"
        )
        
        assert config.table_name == "TestStatic"
        assert config.hash_algorithm == "hashbytes_sha256"
        assert config.load_strategy == "replace"
        
    def test_static_table_upsert_validation(self):
        """Test static table upsert strategy validation."""
        with pytest.raises(ValueError, match="Primary key columns required when load_strategy is 'upsert'"):
            StaticTableConfig(
                table_name="TestStatic",
                target_table="test_target",
                load_strategy="upsert"
            )


class TestPipelineConfig:
    """Test main pipeline configuration."""
    
    def test_valid_pipeline_config(self):
        """Test valid pipeline configuration."""
        database_config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            integrated_security=True
        )
        
        partition_table = PartitionTableConfig(
            table_name="TestPartition",
            target_table="test_partition",
            partition_column="DateCol"
        )
        
        config = PipelineConfig(
            database=database_config,
            partition_tables=[partition_table],
            pipeline_name="test_pipeline"
        )
        
        assert config.pipeline_name == "test_pipeline"
        assert len(config.partition_tables) == 1
        assert config.log_level == "INFO"  # default
        
    def test_pipeline_name_normalization(self):
        """Test pipeline name normalization."""
        database_config = DatabaseConfig(
            server="localhost",
            database="TestDB", 
            integrated_security=True
        )
        
        partition_table = PartitionTableConfig(
            table_name="TestPartition",
            target_table="test_partition",
            partition_column="DateCol"
        )
        
        config = PipelineConfig(
            database=database_config,
            partition_tables=[partition_table],
            pipeline_name="Test-Pipeline Name"
        )
        
        assert config.pipeline_name == "test_pipeline_name"
        
    def test_no_tables_validation(self):
        """Test validation when no tables are configured."""
        database_config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            integrated_security=True
        )
        
        with pytest.raises(ValueError, match="At least one table configuration is required"):
            PipelineConfig(
                database=database_config,
                pipeline_name="test_pipeline"
            )
            
    def test_get_all_table_names(self):
        """Test getting all configured table names."""
        database_config = DatabaseConfig(
            server="localhost",
            database="TestDB",
            integrated_security=True
        )
        
        partition_table = PartitionTableConfig(
            table_name="PartitionTable",
            target_table="partition_target",
            partition_column="DateCol"
        )
        
        scd_table = SCDTableConfig(
            table_name="SCDTable",
            target_table="scd_target",
            business_key_columns=["ID"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsLatest"
        )
        
        static_table = StaticTableConfig(
            table_name="StaticTable",
            target_table="static_target"
        )
        
        config = PipelineConfig(
            database=database_config,
            partition_tables=[partition_table],
            scd_tables=[scd_table],
            static_tables=[static_table],
            pipeline_name="test_pipeline"
        )
        
        table_names = config.get_all_table_names()
        expected_names = ["dbo.PartitionTable", "dbo.SCDTable", "dbo.StaticTable"]
        
        assert set(table_names) == set(expected_names)
        assert len(table_names) == 3


if __name__ == "__main__":
    pytest.main([__file__])
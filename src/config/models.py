"""Configuration models for SQL Server incremental data pipeline."""

from typing import Dict, List, Optional, Union, Literal
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import BaseModel, Field, validator, root_validator
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    
    server: str = Field(..., description="SQL Server hostname or IP address")
    database: str = Field(..., description="Database name")
    username: Optional[str] = Field(None, description="Username (if not using integrated auth)")
    password: Optional[str] = Field(None, description="Password (if not using integrated auth)")
    port: int = Field(1433, description="SQL Server port")
    driver: str = Field("ODBC Driver 18 for SQL Server", description="ODBC driver name")
    trust_server_certificate: bool = Field(True, description="Trust server certificate")
    integrated_security: bool = Field(True, description="Use Windows integrated authentication")
    connection_timeout: int = Field(30, description="Connection timeout in seconds")
    command_timeout: int = Field(300, description="Command timeout in seconds")
    
    @validator("port")
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v
    
    @root_validator
    def validate_auth(cls, values):
        if not values.get("integrated_security") and not all([
            values.get("username"), values.get("password")
        ]):
            raise ValueError("Username and password required when integrated_security is False")
        return values

    def get_connection_string(self) -> str:
        """Generate SQL Server connection string."""
        parts = [
            f"DRIVER={{{self.driver}}}",
            f"SERVER={self.server},{self.port}",
            f"DATABASE={self.database}",
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}",
        ]
        
        if self.integrated_security:
            parts.append("Trusted_Connection=yes")
        else:
            parts.extend([f"UID={self.username}", f"PWD={self.password}"])
            
        if self.connection_timeout:
            parts.append(f"Connection Timeout={self.connection_timeout}")
            
        return ";".join(parts)


class ProcessingConfig(BaseModel):
    """Processing configuration and limits."""
    
    batch_size: int = Field(10000, description="Records per batch")
    max_workers: int = Field(4, description="Maximum parallel workers")
    memory_limit_mb: int = Field(1024, description="Memory limit in MB per worker")
    retry_attempts: int = Field(3, description="Number of retry attempts on failure")
    retry_delay_seconds: int = Field(30, description="Delay between retries in seconds")
    partition_parallel_limit: int = Field(2, description="Max parallel partition operations")
    enable_sql_server_partition_switching: bool = Field(
        True, description="Use SQL Server partition switching when available"
    )
    validate_precision_end_to_end: bool = Field(
        True, description="Validate decimal precision throughout pipeline"
    )
    
    @validator("batch_size", "max_workers", "memory_limit_mb", "retry_attempts", "partition_parallel_limit")
    def validate_positive(cls, v):
        if v <= 0:
            raise ValueError("Value must be positive")
        return v


class PartitionTableConfig(BaseModel):
    """Configuration for partition-based incremental tables."""
    
    table_name: str = Field(..., description="Source table name")
    partition_column: str = Field("COBID", description="Column used for partitioning")
    target_table: str = Field(..., description="Target table name in destination")
    schema_name: str = Field("dbo", description="Source schema name")
    target_schema: str = Field("dbo", description="Target schema name") 
    batch_size: Optional[int] = Field(None, description="Override default batch size")
    enable_partition_switching: Optional[bool] = Field(None, description="Override partition switching setting")
    partition_function: Optional[str] = Field(None, description="SQL Server partition function name")
    partition_scheme: Optional[str] = Field(None, description="SQL Server partition scheme name")
    custom_where_clause: Optional[str] = Field(None, description="Additional WHERE clause filters")
    
    @validator("table_name", "target_table", "partition_column")
    def validate_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Value cannot be empty")
        return v.strip()


class SCDTableConfig(BaseModel):
    """Configuration for SCD Type 2 tables."""
    
    table_name: str = Field(..., description="Source table name")
    business_key_columns: List[str] = Field(..., description="Business key column names")
    last_modified_column: str = Field("LastModified", description="Last modified timestamp column")
    is_latest_column: str = Field("IsLatest", description="Current record indicator column")
    target_table: str = Field(..., description="Target table name in destination")
    schema_name: str = Field("dbo", description="Source schema name")
    target_schema: str = Field("dbo", description="Target schema name")
    batch_size: Optional[int] = Field(None, description="Override default batch size")
    effective_date_column: Optional[str] = Field(None, description="Effective date column name")
    end_date_column: Optional[str] = Field(None, description="End date column name")
    custom_where_clause: Optional[str] = Field(None, description="Additional WHERE clause filters")
    
    @validator("table_name", "target_table", "last_modified_column", "is_latest_column")
    def validate_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Value cannot be empty")
        return v.strip()
    
    @validator("business_key_columns")
    def validate_business_keys(cls, v):
        if not v or len(v) == 0:
            raise ValueError("At least one business key column is required")
        return [col.strip() for col in v if col.strip()]


class StaticTableConfig(BaseModel):
    """Configuration for static reference tables."""
    
    table_name: str = Field(..., description="Source table name")
    target_table: str = Field(..., description="Target table name in destination")
    schema_name: str = Field("dbo", description="Source schema name")
    target_schema: str = Field("dbo", description="Target schema name")
    hash_algorithm: Literal["checksum", "hashbytes_md5", "hashbytes_sha256"] = Field(
        "hashbytes_sha256", description="Hash algorithm for change detection"
    )
    load_strategy: Literal["replace", "upsert"] = Field(
        "replace", description="Loading strategy"
    )
    primary_key_columns: Optional[List[str]] = Field(None, description="Primary key columns for upsert")
    batch_size: Optional[int] = Field(None, description="Override default batch size")
    force_reload_after_days: Optional[int] = Field(None, description="Force reload after N days")
    custom_where_clause: Optional[str] = Field(None, description="Additional WHERE clause filters")
    
    @validator("table_name", "target_table")
    def validate_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Value cannot be empty")
        return v.strip()
    
    @root_validator
    def validate_upsert_requirements(cls, values):
        if values.get("load_strategy") == "upsert" and not values.get("primary_key_columns"):
            raise ValueError("Primary key columns required when load_strategy is 'upsert'")
        return values


class StateConfig(BaseModel):
    """State management configuration."""
    
    state_directory: Path = Field(Path("./state"), description="Directory for state files")
    parquet_directory: Path = Field(Path("./data"), description="Directory for Parquet files")
    enable_self_healing: bool = Field(True, description="Enable self-healing state detection")
    state_retention_days: int = Field(30, description="Days to retain old state files")
    validate_state_consistency: bool = Field(True, description="Validate state consistency on startup")
    backup_state_before_run: bool = Field(True, description="Backup state before each run")
    
    @validator("state_retention_days")
    def validate_retention(cls, v):
        if v <= 0:
            raise ValueError("State retention days must be positive")
        return v


class PipelineConfig(BaseSettings):
    """Main pipeline configuration."""
    
    database: DatabaseConfig = Field(..., description="Database connection settings")
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig, description="Processing settings")
    state: StateConfig = Field(default_factory=StateConfig, description="State management settings")
    
    partition_tables: List[PartitionTableConfig] = Field(
        default_factory=list, description="Partition-based tables configuration"
    )
    scd_tables: List[SCDTableConfig] = Field(
        default_factory=list, description="SCD Type 2 tables configuration"
    )
    static_tables: List[StaticTableConfig] = Field(
        default_factory=list, description="Static reference tables configuration"
    )
    
    pipeline_name: str = Field("sql_server_incremental", description="DLT pipeline name")
    destination: str = Field("parquet", description="DLT destination type")
    destination_config: Dict = Field(default_factory=dict, description="Destination-specific configuration")
    
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        "INFO", description="Logging level"
    )
    enable_structured_logging: bool = Field(True, description="Enable structured JSON logging")
    log_file: Optional[Path] = Field(None, description="Log file path (None for console only)")
    
    class Config:
        env_prefix = "DLT_SYNC_"
        env_file = ".env"
        env_file_encoding = "utf-8"
    
    @validator("pipeline_name")
    def validate_pipeline_name(cls, v):
        if not v.strip():
            raise ValueError("Pipeline name cannot be empty")
        # DLT pipeline names should be lowercase with underscores
        return v.lower().replace("-", "_").replace(" ", "_")
    
    @root_validator
    def validate_has_tables(cls, values):
        if not any([
            values.get("partition_tables"),
            values.get("scd_tables"), 
            values.get("static_tables")
        ]):
            raise ValueError("At least one table configuration is required")
        return values
    
    def get_all_table_names(self) -> List[str]:
        """Get all configured table names."""
        names = []
        for config in self.partition_tables:
            names.append(f"{config.schema_name}.{config.table_name}")
        for config in self.scd_tables:
            names.append(f"{config.schema_name}.{config.table_name}")
        for config in self.static_tables:
            names.append(f"{config.schema_name}.{config.table_name}")
        return names
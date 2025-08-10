"""Data validation utilities for the SQL Server incremental pipeline."""

from typing import List, Dict, Any, Optional, Union, Tuple
from decimal import Decimal, getcontext, ROUND_HALF_UP
import pyarrow as pa
import pandas as pd
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import Engine
import structlog

from ..config.models import DatabaseConfig

logger = structlog.get_logger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


def validate_decimal_precision(
    value: Union[Decimal, float, str], 
    expected_precision: int, 
    expected_scale: int
) -> bool:
    """
    Validate that a decimal value matches expected precision and scale.
    
    Args:
        value: Decimal value to validate
        expected_precision: Expected total number of digits
        expected_scale: Expected number of digits after decimal point
        
    Returns:
        True if validation passes
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Convert to Decimal for precise validation
        if isinstance(value, (float, str)):
            decimal_value = Decimal(str(value))
        elif isinstance(value, Decimal):
            decimal_value = value
        else:
            raise ValidationError(f"Unsupported decimal type: {type(value)}")
        
        # Get precision and scale of the value
        sign, digits, exponent = decimal_value.as_tuple()
        
        if exponent >= 0:
            # No decimal places
            actual_precision = len(digits)
            actual_scale = 0
        else:
            # Has decimal places
            actual_precision = len(digits)
            actual_scale = -exponent
        
        # Validate precision and scale
        if actual_precision > expected_precision:
            raise ValidationError(
                f"Decimal precision {actual_precision} exceeds expected {expected_precision}"
            )
        
        if actual_scale > expected_scale:
            raise ValidationError(
                f"Decimal scale {actual_scale} exceeds expected {expected_scale}"
            )
        
        return True
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating decimal precision: {str(e)}")


def validate_table_exists(engine: Engine, schema_name: str, table_name: str) -> bool:
    """
    Validate that a table exists in the database.
    
    Args:
        engine: SQLAlchemy engine
        schema_name: Schema name
        table_name: Table name
        
    Returns:
        True if table exists
        
    Raises:
        ValidationError: If table does not exist
    """
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=schema_name)
        
        if table_name not in tables:
            raise ValidationError(
                f"Table {schema_name}.{table_name} does not exist. "
                f"Available tables: {', '.join(tables)}"
            )
        
        return True
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating table existence: {str(e)}")


def validate_column_exists(
    engine: Engine, 
    schema_name: str, 
    table_name: str, 
    column_names: Union[str, List[str]]
) -> bool:
    """
    Validate that columns exist in a table.
    
    Args:
        engine: SQLAlchemy engine
        schema_name: Schema name
        table_name: Table name
        column_names: Column name(s) to validate
        
    Returns:
        True if all columns exist
        
    Raises:
        ValidationError: If any column does not exist
    """
    try:
        if isinstance(column_names, str):
            column_names = [column_names]
        
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name, schema=schema_name)
        existing_columns = [col['name'] for col in columns]
        
        missing_columns = [col for col in column_names if col not in existing_columns]
        
        if missing_columns:
            raise ValidationError(
                f"Columns {missing_columns} do not exist in {schema_name}.{table_name}. "
                f"Available columns: {', '.join(existing_columns)}"
            )
        
        return True
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating column existence: {str(e)}")


def validate_partition_configuration(
    engine: Engine,
    schema_name: str,
    table_name: str,
    partition_column: str,
    partition_function: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate partition configuration for a table.
    
    Args:
        engine: SQLAlchemy engine
        schema_name: Schema name
        table_name: Table name
        partition_column: Partition column name
        partition_function: Expected partition function name
        
    Returns:
        Dictionary with partition information
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # First validate table and column exist
        validate_table_exists(engine, schema_name, table_name)
        validate_column_exists(engine, schema_name, table_name, partition_column)
        
        # Query partition information
        query = text("""
            SELECT 
                pf.name AS partition_function,
                ps.name AS partition_scheme,
                p.partition_number,
                p.rows,
                rv.value AS boundary_value
            FROM sys.tables t
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            INNER JOIN sys.indexes i ON t.object_id = i.object_id AND p.index_id = i.index_id
            INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
            LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id 
                AND p.partition_number = rv.boundary_id + 1
            WHERE t.name = :table_name
                AND SCHEMA_NAME(t.schema_id) = :schema_name
                AND i.index_id <= 1
            ORDER BY p.partition_number
        """)
        
        with engine.connect() as conn:
            result = conn.execute(
                query, 
                {"table_name": table_name, "schema_name": schema_name}
            ).fetchall()
        
        if not result:
            return {
                "is_partitioned": False,
                "partition_function": None,
                "partition_scheme": None,
                "partitions": []
            }
        
        # Validate partition function if specified
        if partition_function and result[0].partition_function != partition_function:
            raise ValidationError(
                f"Expected partition function {partition_function}, "
                f"but found {result[0].partition_function}"
            )
        
        partitions = []
        for row in result:
            partitions.append({
                "partition_number": row.partition_number,
                "row_count": row.rows,
                "boundary_value": row.boundary_value
            })
        
        return {
            "is_partitioned": True,
            "partition_function": result[0].partition_function,
            "partition_scheme": result[0].partition_scheme,
            "partitions": partitions
        }
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating partition configuration: {str(e)}")


def validate_scd_configuration(
    engine: Engine,
    schema_name: str,
    table_name: str,
    business_key_columns: List[str],
    last_modified_column: str,
    is_latest_column: str
) -> Dict[str, Any]:
    """
    Validate SCD Type 2 configuration for a table.
    
    Args:
        engine: SQLAlchemy engine
        schema_name: Schema name
        table_name: Table name
        business_key_columns: Business key column names
        last_modified_column: Last modified column name
        is_latest_column: Is latest indicator column name
        
    Returns:
        Dictionary with SCD validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Validate table and columns exist
        validate_table_exists(engine, schema_name, table_name)
        all_columns = business_key_columns + [last_modified_column, is_latest_column]
        validate_column_exists(engine, schema_name, table_name, all_columns)
        
        # Check for unique constraint on business key + is_latest
        constraint_query = text("""
            SELECT 
                kc.CONSTRAINT_NAME,
                STRING_AGG(kc.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kc.ORDINAL_POSITION) AS columns
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc 
                ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA
                AND tc.TABLE_NAME = kc.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = :schema_name
                AND tc.TABLE_NAME = :table_name
                AND tc.CONSTRAINT_TYPE = 'UNIQUE'
            GROUP BY kc.CONSTRAINT_NAME
        """)
        
        with engine.connect() as conn:
            constraints = conn.execute(
                constraint_query,
                {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
        
        # Check data integrity - should have at most one IsLatest=1 per business key
        business_key_cols = ", ".join(business_key_columns)
        integrity_query = text(f"""
            SELECT {business_key_cols}, COUNT(*) as latest_count
            FROM [{schema_name}].[{table_name}]
            WHERE [{is_latest_column}] = 1
            GROUP BY {business_key_cols}
            HAVING COUNT(*) > 1
        """)
        
        with engine.connect() as conn:
            integrity_violations = conn.execute(integrity_query).fetchall()
        
        # Get sample data for analysis
        sample_query = text(f"""
            SELECT TOP 10 
                {business_key_cols},
                [{last_modified_column}],
                [{is_latest_column}]
            FROM [{schema_name}].[{table_name}]
            ORDER BY [{last_modified_column}] DESC
        """)
        
        with engine.connect() as conn:
            sample_data = conn.execute(sample_query).fetchall()
        
        return {
            "unique_constraints": [
                {"name": row.CONSTRAINT_NAME, "columns": row.columns}
                for row in constraints
            ],
            "integrity_violations_count": len(integrity_violations),
            "integrity_violations": [dict(row._mapping) for row in integrity_violations[:10]],
            "sample_records": [dict(row._mapping) for row in sample_data],
            "validation_passed": len(integrity_violations) == 0
        }
        
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating SCD configuration: {str(e)}")


def validate_arrow_schema_compatibility(
    source_schema: Dict[str, Any], 
    arrow_schema: pa.Schema
) -> List[str]:
    """
    Validate compatibility between source database schema and Arrow schema.
    
    Args:
        source_schema: Database schema information
        arrow_schema: PyArrow schema
        
    Returns:
        List of compatibility warnings/errors
    """
    issues = []
    
    try:
        arrow_fields = {field.name: field for field in arrow_schema}
        
        for col_name, col_info in source_schema.items():
            if col_name not in arrow_fields:
                issues.append(f"Column {col_name} missing from Arrow schema")
                continue
            
            arrow_field = arrow_fields[col_name]
            source_type = col_info.get("data_type", "").upper()
            
            # Check decimal precision compatibility
            if "DECIMAL" in source_type or "NUMERIC" in source_type:
                if not pa.types.is_decimal(arrow_field.type):
                    issues.append(
                        f"Column {col_name}: Source is decimal but Arrow type is {arrow_field.type}"
                    )
                else:
                    source_precision = col_info.get("precision", 0)
                    source_scale = col_info.get("scale", 0)
                    
                    if (arrow_field.type.precision != source_precision or 
                        arrow_field.type.scale != source_scale):
                        issues.append(
                            f"Column {col_name}: Precision/scale mismatch. "
                            f"Source: {source_precision}/{source_scale}, "
                            f"Arrow: {arrow_field.type.precision}/{arrow_field.type.scale}"
                        )
        
        return issues
        
    except Exception as e:
        return [f"Error validating schema compatibility: {str(e)}"]


def validate_database_connectivity(config: DatabaseConfig) -> bool:
    """
    Validate database connectivity with the given configuration.
    
    Args:
        config: Database configuration
        
    Returns:
        True if connection successful
        
    Raises:
        ValidationError: If connection fails
    """
    try:
        connection_string = f"mssql+pyodbc:///?odbc_connect={config.get_connection_string()}"
        engine = create_engine(connection_string, echo=False)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test")).fetchone()
            if result[0] != 1:
                raise ValidationError("Unexpected result from database connectivity test")
        
        logger.info("Database connectivity validation successful")
        return True
        
    except Exception as e:
        raise ValidationError(f"Database connectivity validation failed: {str(e)}")
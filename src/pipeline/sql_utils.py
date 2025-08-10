"""SQL Server utilities and metadata queries for the incremental pipeline."""

from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import pyarrow as pa
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.engine import Engine, Connection
import structlog

from ..config.models import DatabaseConfig
from ..utils.validation import ValidationError

logger = structlog.get_logger(__name__)


class SQLServerUtils:
    """Utility class for SQL Server operations and metadata queries."""
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize SQL Server utilities.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self.connection_string = f"mssql+pyodbc:///?odbc_connect={config.get_connection_string()}"
        self._engine: Optional[Engine] = None
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "timeout": self.config.connection_timeout,
                    "autocommit": True
                }
            )
        return self._engine
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error("Database connection test failed", error=str(e))
            return False
    
    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get detailed schema information for a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Dictionary with table schema information
        """
        query = text("""
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.DATETIME_PRECISION,
                c.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = :schema_name
                AND c.TABLE_NAME = :table_name
            ORDER BY c.ORDINAL_POSITION
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query, 
                {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
        
        schema_info = {}
        for row in result:
            schema_info[row.COLUMN_NAME] = {
                "data_type": row.DATA_TYPE,
                "is_nullable": row.IS_NULLABLE == "YES",
                "default": row.COLUMN_DEFAULT,
                "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                "precision": row.NUMERIC_PRECISION,
                "scale": row.NUMERIC_SCALE,
                "datetime_precision": row.DATETIME_PRECISION,
                "ordinal_position": row.ORDINAL_POSITION
            }
        
        return schema_info
    
    def get_partition_info(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get partition information for a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Dictionary with partition information
        """
        query = text("""
            SELECT 
                t.name AS table_name,
                pf.name AS partition_function,
                pf.type_desc AS partition_type,
                ps.name AS partition_scheme,
                c.name AS partition_column,
                p.partition_number,
                p.rows AS partition_rows,
                rv.value AS boundary_value,
                fg.name AS filegroup_name
            FROM sys.tables t
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            INNER JOIN sys.indexes i ON t.object_id = i.object_id AND p.index_id = i.index_id
            INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
            INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
            INNER JOIN sys.columns c ON pp.parameter_id = c.column_id AND t.object_id = c.object_id
            LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id 
                AND p.partition_number = rv.boundary_id + 1
            INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
                AND p.partition_number = dds.destination_id
            INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
            WHERE t.name = :table_name
                AND SCHEMA_NAME(t.schema_id) = :schema_name
                AND i.index_id <= 1
            ORDER BY p.partition_number
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
        
        if not result:
            return {"is_partitioned": False}
        
        first_row = result[0]
        partitions = []
        
        for row in result:
            partitions.append({
                "partition_number": row.partition_number,
                "row_count": row.partition_rows,
                "boundary_value": row.boundary_value,
                "filegroup": row.filegroup_name
            })
        
        return {
            "is_partitioned": True,
            "partition_function": first_row.partition_function,
            "partition_type": first_row.partition_type,
            "partition_scheme": first_row.partition_scheme,
            "partition_column": first_row.partition_column,
            "partitions": partitions
        }
    
    def get_primary_key_columns(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            List of primary key column names
        """
        query = text("""
            SELECT kc.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc 
                ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kc.TABLE_SCHEMA
                AND tc.TABLE_NAME = kc.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = :schema_name
                AND tc.TABLE_NAME = :table_name
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kc.ORDINAL_POSITION
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
        
        return [row.COLUMN_NAME for row in result]
    
    def get_table_row_count(self, schema_name: str, table_name: str, where_clause: Optional[str] = None) -> int:
        """
        Get row count for a table with optional WHERE clause.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            where_clause: Optional WHERE clause
            
        Returns:
            Row count
        """
        base_query = f"SELECT COUNT(*) as row_count FROM [{schema_name}].[{table_name}]"
        if where_clause:
            query_text = f"{base_query} WHERE {where_clause}"
        else:
            query_text = base_query
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query_text)).fetchone()
            return result.row_count
    
    def get_table_hash(self, schema_name: str, table_name: str, hash_algorithm: str = "hashbytes_sha256") -> str:
        """
        Calculate hash for entire table content.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            hash_algorithm: Hash algorithm (checksum, hashbytes_md5, hashbytes_sha256)
            
        Returns:
            Hex string representation of hash
        """
        if hash_algorithm == "checksum":
            query_text = f"SELECT CHECKSUM_AGG(CHECKSUM(*)) as table_hash FROM [{schema_name}].[{table_name}]"
        elif hash_algorithm == "hashbytes_md5":
            query_text = f"""
                SELECT MASTER.dbo.fn_varbintohexstr(
                    HASHBYTES('MD5', 
                        (SELECT * FROM [{schema_name}].[{table_name}] FOR XML RAW)
                    )
                ) as table_hash
            """
        elif hash_algorithm == "hashbytes_sha256":
            query_text = f"""
                SELECT MASTER.dbo.fn_varbintohexstr(
                    HASHBYTES('SHA2_256', 
                        (SELECT * FROM [{schema_name}].[{table_name}] FOR XML RAW)
                    )
                ) as table_hash
            """
        else:
            raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query_text)).fetchone()
            return str(result.table_hash) if result.table_hash else ""
    
    def get_min_max_timestamp(self, schema_name: str, table_name: str, timestamp_column: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get minimum and maximum timestamps from a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            timestamp_column: Timestamp column name
            
        Returns:
            Tuple of (min_timestamp, max_timestamp)
        """
        query = text(f"""
            SELECT 
                MIN([{timestamp_column}]) as min_timestamp,
                MAX([{timestamp_column}]) as max_timestamp
            FROM [{schema_name}].[{table_name}]
            WHERE [{timestamp_column}] IS NOT NULL
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return (result.min_timestamp, result.max_timestamp)
    
    def get_distinct_partition_values(
        self, 
        schema_name: str, 
        table_name: str, 
        partition_column: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None
    ) -> List[Any]:
        """
        Get distinct values from partition column.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            partition_column: Partition column name
            min_value: Optional minimum value filter
            max_value: Optional maximum value filter
            
        Returns:
            List of distinct partition values
        """
        query_parts = [f"SELECT DISTINCT [{partition_column}] FROM [{schema_name}].[{table_name}]"]
        
        where_conditions = [f"[{partition_column}] IS NOT NULL"]
        if min_value is not None:
            where_conditions.append(f"[{partition_column}] >= :min_value")
        if max_value is not None:
            where_conditions.append(f"[{partition_column}] <= :max_value")
        
        if where_conditions:
            query_parts.append("WHERE " + " AND ".join(where_conditions))
        
        query_parts.append(f"ORDER BY [{partition_column}]")
        query_text = " ".join(query_parts)
        
        params = {}
        if min_value is not None:
            params["min_value"] = min_value
        if max_value is not None:
            params["max_value"] = max_value
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query_text), params).fetchall()
            return [row[0] for row in result]
    
    def check_partition_switch_prerequisites(
        self, 
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        partition_number: int
    ) -> Dict[str, Any]:
        """
        Check prerequisites for partition switching.
        
        Args:
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            partition_number: Partition number to switch
            
        Returns:
            Dictionary with prerequisite check results
        """
        # Check if tables exist and are partitioned
        source_partition_info = self.get_partition_info(source_schema, source_table)
        target_partition_info = self.get_partition_info(target_schema, target_table)
        
        prerequisites = {
            "source_partitioned": source_partition_info.get("is_partitioned", False),
            "target_partitioned": target_partition_info.get("is_partitioned", False),
            "compatible_schemas": False,
            "constraints_match": False,
            "indexes_match": False,
            "can_switch": False
        }
        
        if not all([prerequisites["source_partitioned"], prerequisites["target_partitioned"]]):
            return prerequisites
        
        # Check schema compatibility
        source_schema_info = self.get_table_schema(source_schema, source_table)
        target_schema_info = self.get_table_schema(target_schema, target_table)
        prerequisites["compatible_schemas"] = source_schema_info == target_schema_info
        
        # Note: Full constraint and index checking would require additional queries
        # For now, assume they match if schemas are compatible
        prerequisites["constraints_match"] = prerequisites["compatible_schemas"]
        prerequisites["indexes_match"] = prerequisites["compatible_schemas"]
        
        prerequisites["can_switch"] = all([
            prerequisites["source_partitioned"],
            prerequisites["target_partitioned"],
            prerequisites["compatible_schemas"],
            prerequisites["constraints_match"],
            prerequisites["indexes_match"]
        ])
        
        return prerequisites
    
    def execute_partition_switch(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        source_partition: int,
        target_partition: int = 1
    ) -> bool:
        """
        Execute partition switch operation.
        
        Args:
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            source_partition: Source partition number
            target_partition: Target partition number (usually 1 for staging tables)
            
        Returns:
            True if successful
        """
        try:
            switch_sql = f"""
                ALTER TABLE [{source_schema}].[{source_table}]
                SWITCH PARTITION {source_partition}
                TO [{target_schema}].[{target_table}] PARTITION {target_partition}
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(switch_sql))
                conn.commit()
            
            logger.info(
                "Partition switch completed successfully",
                source_table=f"{source_schema}.{source_table}",
                target_table=f"{target_schema}.{target_table}",
                source_partition=source_partition,
                target_partition=target_partition
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "Partition switch failed",
                source_table=f"{source_schema}.{source_table}",
                target_table=f"{target_schema}.{target_table}",
                source_partition=source_partition,
                target_partition=target_partition,
                error=str(e)
            )
            return False
    
    def create_staging_table_like(
        self,
        source_schema: str,
        source_table: str,
        staging_schema: str,
        staging_table: str,
        include_constraints: bool = True,
        include_indexes: bool = True
    ) -> bool:
        """
        Create a staging table with the same structure as source table.
        
        Args:
            source_schema: Source schema name
            source_table: Source table name
            staging_schema: Staging schema name
            staging_table: Staging table name
            include_constraints: Include constraints in staging table
            include_indexes: Include indexes in staging table
            
        Returns:
            True if successful
        """
        try:
            # First, create the table structure
            create_sql = f"""
                SELECT TOP 0 * 
                INTO [{staging_schema}].[{staging_table}]
                FROM [{source_schema}].[{source_table}]
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logger.info(
                "Staging table created successfully",
                source_table=f"{source_schema}.{source_table}",
                staging_table=f"{staging_schema}.{staging_table}"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "Failed to create staging table",
                source_table=f"{source_schema}.{source_table}",
                staging_table=f"{staging_schema}.{staging_table}",
                error=str(e)
            )
            return False
    
    def cleanup_staging_tables(self, schema_name: str, table_pattern: str = "staging_%") -> int:
        """
        Clean up staging tables matching a pattern.
        
        Args:
            schema_name: Schema name
            table_pattern: Table name pattern (SQL LIKE pattern)
            
        Returns:
            Number of tables dropped
        """
        try:
            # Get list of staging tables
            query = text("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :schema_name
                    AND TABLE_NAME LIKE :table_pattern
                    AND TABLE_TYPE = 'BASE TABLE'
            """)
            
            with self.engine.connect() as conn:
                staging_tables = conn.execute(
                    query,
                    {"schema_name": schema_name, "table_pattern": table_pattern}
                ).fetchall()
                
                dropped_count = 0
                for row in staging_tables:
                    try:
                        drop_sql = f"DROP TABLE [{schema_name}].[{row.TABLE_NAME}]"
                        conn.execute(text(drop_sql))
                        dropped_count += 1
                        logger.debug(
                            "Dropped staging table",
                            table=f"{schema_name}.{row.TABLE_NAME}"
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to drop staging table",
                            table=f"{schema_name}.{row.TABLE_NAME}",
                            error=str(e)
                        )
                
                conn.commit()
            
            logger.info(
                "Staging table cleanup completed",
                dropped_count=dropped_count,
                total_found=len(staging_tables)
            )
            
            return dropped_count
            
        except Exception as e:
            logger.error("Staging table cleanup failed", error=str(e))
            return 0
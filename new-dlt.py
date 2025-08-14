"""
DLT Pipeline for MSSQL Production to Staging with Arrow format
Supports: Incremental loads, Full refresh, and SCD table updates
"""

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from typing import Iterator, Optional, Dict, Any, List
import pyodbc
import pyarrow as pa
import pandas as pd
from datetime import datetime
import yaml
import logging
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LoadType(Enum):
    """Enum for different load types"""
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"
    SCD = "scd"


class MSSQLArrowPipeline:
    """Main pipeline class for MSSQL to MSSQL transfers using Arrow format"""
    
    def __init__(self, prod_conn_string: str, staging_conn_string: str):
        self.prod_conn_string = prod_conn_string
        self.staging_conn_string = staging_conn_string
    
    def get_table_schema(self, connection_string: str, schema_name: str, table_name: str) -> Optional[pa.Schema]:
        """
        Get the Arrow schema from the source table to ensure type consistency.
        """
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                schema_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """
                
                cursor.execute(schema_query, (schema_name, table_name))
                
                arrow_fields = []
                for row in cursor:
                    col_name = row.COLUMN_NAME
                    sql_type = row.DATA_TYPE.lower()
                    is_nullable = row.IS_NULLABLE == 'YES'
                    
                    # Map SQL Server types to Arrow types
                    arrow_type = self._map_sql_to_arrow_type(
                        sql_type, 
                        row.NUMERIC_PRECISION, 
                        row.NUMERIC_SCALE
                    )
                    
                    arrow_fields.append(pa.field(col_name, arrow_type, nullable=is_nullable))
                
                return pa.schema(arrow_fields)
                
        except Exception as e:
            logger.error(f"Error getting table schema for {schema_name}.{table_name}: {e}")
            return None
    
    def _map_sql_to_arrow_type(self, sql_type: str, precision: Optional[int], scale: Optional[int]) -> pa.DataType:
        """Map SQL Server types to Arrow types"""
        if sql_type in ['int', 'integer']:
            return pa.int32()
        elif sql_type == 'bigint':
            return pa.int64()
        elif sql_type == 'smallint':
            return pa.int16()
        elif sql_type == 'tinyint':
            return pa.int8()
        elif sql_type == 'bit':
            return pa.bool_()
        elif sql_type in ['decimal', 'numeric']:
            precision = precision or 18
            scale = scale or 0
            return pa.decimal128(precision, scale)
        elif sql_type in ['float', 'real']:
            return pa.float64() if sql_type == 'float' else pa.float32()
        elif sql_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text', 'ntext']:
            return pa.string()
        elif sql_type == 'date':
            return pa.date32()
        elif sql_type in ['datetime', 'datetime2', 'smalldatetime']:
            return pa.timestamp('ms')
        elif sql_type == 'time':
            return pa.time64('ns')
        elif sql_type in ['binary', 'varbinary', 'image']:
            return pa.binary()
        elif sql_type == 'uniqueidentifier':
            return pa.string()
        else:
            return pa.string()
    
    def get_max_value(self, schema_name: str, table_name: str, column_name: str) -> Any:
        """Get the maximum value of a column from staging table"""
        try:
            with pyodbc.connect(self.staging_conn_string) as conn:
                cursor = conn.cursor()
                query = f"""
                    SELECT COALESCE(MAX({column_name}), 0) as max_value 
                    FROM {schema_name}.{table_name}
                """
                cursor.execute(query)
                result = cursor.fetchone()
                return result.max_value if result else 0
        except Exception as e:
            logger.warning(f"Could not get max value for {schema_name}.{table_name}.{column_name}: {e}")
            return 0
    
    def truncate_table(self, schema_name: str, table_name: str):
        """Truncate staging table for full refresh"""
        try:
            with pyodbc.connect(self.staging_conn_string) as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
                conn.commit()
                logger.info(f"Truncated table {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Error truncating table {schema_name}.{table_name}: {e}")
            raise
    
    def update_scd_records(self, schema_name: str, table_name: str, 
                          scd_config: Dict[str, Any], new_records_df: pd.DataFrame):
        """
        Update existing SCD records in staging when newer versions arrive
        Sets IsLatest = 0 and updates EndDate for superseded records
        """
        if new_records_df.empty:
            return
        
        try:
            with pyodbc.connect(self.staging_conn_string) as conn:
                cursor = conn.cursor()
                
                # Get the primary key column(s)
                primary_keys = scd_config.get('primary_keys', ['Id'])
                is_latest_col = scd_config.get('is_latest_column', 'IsLatest')
                end_date_col = scd_config.get('end_date_column', 'EndDate')
                
                # Build the update query for each new record
                for _, row in new_records_df.iterrows():
                    # Build WHERE clause for primary keys
                    where_conditions = []
                    params = []
                    
                    for pk in primary_keys:
                        where_conditions.append(f"{pk} = ?")
                        params.append(row[pk])
                    
                    # Update previous versions
                    update_query = f"""
                        UPDATE {schema_name}.{table_name}
                        SET {is_latest_col} = 0
                    """
                    
                    # Add EndDate update if column exists
                    if end_date_col and end_date_col in new_records_df.columns:
                        update_query += f", {end_date_col} = GETDATE()"
                    
                    update_query += f"""
                        WHERE {' AND '.join(where_conditions)}
                        AND {is_latest_col} = 1
                    """
                    
                    cursor.execute(update_query, params)
                
                conn.commit()
                logger.info(f"Updated {len(new_records_df)} SCD records in {schema_name}.{table_name}")
                
        except Exception as e:
            logger.error(f"Error updating SCD records: {e}")
            raise
    
    def load_incremental(self, table_config: Dict[str, Any], batch_size: int = 10000) -> Iterator[pa.Table]:
        """Load incremental data based on a watermark column"""
        schema_name = table_config['schema']
        table_name = table_config['table']
        watermark_column = table_config['watermark_column']
        full_table_name = f"{schema_name}.{table_name}"
        
        # Get current maximum value from staging
        max_value = self.get_max_value(schema_name, table_name, watermark_column)
        logger.info(f"Maximum {watermark_column} in staging {full_table_name}: {max_value}")
        
        # Get schema for type consistency
        arrow_schema = self.get_table_schema(self.prod_conn_string, schema_name, table_name)
        
        with pyodbc.connect(self.prod_conn_string) as conn:
            # Count records to transfer
            count_query = f"""
                SELECT COUNT(*) as record_count 
                FROM {full_table_name}
                WHERE {watermark_column} > ?
            """
            cursor = conn.cursor()
            cursor.execute(count_query, max_value)
            total_records = cursor.fetchone().record_count
            logger.info(f"Found {total_records} new records to transfer for {full_table_name}")
            
            if total_records == 0:
                return
            
            # Fetch records in batches
            query = f"""
                SELECT * 
                FROM {full_table_name}
                WHERE {watermark_column} > ?
                ORDER BY {watermark_column}
            """
            
            for chunk_df in pd.read_sql_query(
                query, 
                conn, 
                params=[max_value],
                chunksize=batch_size
            ):
                # Convert to Arrow Table with schema
                if arrow_schema:
                    try:
                        arrow_table = pa.Table.from_pandas(
                            chunk_df, 
                            schema=arrow_schema,
                            preserve_index=False
                        )
                    except Exception as e:
                        logger.warning(f"Could not apply schema: {e}")
                        arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                else:
                    arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                
                logger.info(f"Processing batch of {len(arrow_table)} records for {full_table_name}")
                yield arrow_table
    
    def load_full_refresh(self, table_config: Dict[str, Any], batch_size: int = 10000) -> Iterator[pa.Table]:
        """Load all data from source table (after truncating staging)"""
        schema_name = table_config['schema']
        table_name = table_config['table']
        full_table_name = f"{schema_name}.{table_name}"
        
        # Truncate staging table first
        self.truncate_table(schema_name, table_name)
        
        # Get schema for type consistency
        arrow_schema = self.get_table_schema(self.prod_conn_string, schema_name, table_name)
        
        with pyodbc.connect(self.prod_conn_string) as conn:
            query = f"SELECT * FROM {full_table_name}"
            
            for chunk_df in pd.read_sql_query(query, conn, chunksize=batch_size):
                # Convert to Arrow Table with schema
                if arrow_schema:
                    try:
                        arrow_table = pa.Table.from_pandas(
                            chunk_df, 
                            schema=arrow_schema,
                            preserve_index=False
                        )
                    except Exception as e:
                        logger.warning(f"Could not apply schema: {e}")
                        arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                else:
                    arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                
                logger.info(f"Processing batch of {len(arrow_table)} records for {full_table_name}")
                yield arrow_table
    
    def load_scd(self, table_config: Dict[str, Any], batch_size: int = 10000) -> Iterator[pa.Table]:
        """Load SCD table with proper handling of IsLatest flags and EndDate"""
        schema_name = table_config['schema']
        table_name = table_config['table']
        full_table_name = f"{schema_name}.{table_name}"
        scd_config = table_config.get('scd_config', {})
        
        # Get the modified date column
        modified_column = scd_config.get('modified_column', 'LastModified')
        
        # Get current maximum modified date from staging
        max_value = self.get_max_value(schema_name, table_name, modified_column)
        logger.info(f"Maximum {modified_column} in staging {full_table_name}: {max_value}")
        
        # Get schema for type consistency
        arrow_schema = self.get_table_schema(self.prod_conn_string, schema_name, table_name)
        
        with pyodbc.connect(self.prod_conn_string) as conn:
            # Get new/updated records
            query = f"""
                SELECT * 
                FROM {full_table_name}
                WHERE {modified_column} > ?
                ORDER BY {modified_column}
            """
            
            all_new_records = pd.read_sql_query(query, conn, params=[max_value])
            
            if not all_new_records.empty:
                logger.info(f"Found {len(all_new_records)} new/updated SCD records for {full_table_name}")
                
                # Update existing records in staging (set IsLatest = 0, update EndDate)
                self.update_scd_records(schema_name, table_name, scd_config, all_new_records)
                
                # Yield new records in batches
                for i in range(0, len(all_new_records), batch_size):
                    chunk_df = all_new_records.iloc[i:i+batch_size]
                    
                    # Convert to Arrow Table with schema
                    if arrow_schema:
                        try:
                            arrow_table = pa.Table.from_pandas(
                                chunk_df, 
                                schema=arrow_schema,
                                preserve_index=False
                            )
                        except Exception as e:
                            logger.warning(f"Could not apply schema: {e}")
                            arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                    else:
                        arrow_table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                    
                    logger.info(f"Processing SCD batch of {len(arrow_table)} records for {full_table_name}")
                    yield arrow_table
            else:
                logger.info(f"No new SCD records found for {full_table_name}")


@dlt.source
def mssql_arrow_source(pipeline: MSSQLArrowPipeline, table_configs: List[Dict[str, Any]]):
    """
    DLT source that processes multiple tables based on configuration
    """
    
    for table_config in table_configs:
        schema_name = table_config['schema']
        table_name = table_config['table']
        load_type = LoadType(table_config['load_type'])
        resource_name = f"{schema_name}_{table_name}".replace(".", "_")
        
        # Determine write disposition based on load type
        if load_type == LoadType.FULL_REFRESH:
            write_disposition = "replace"
        else:
            write_disposition = "append"
        
        @dlt.resource(
            name=resource_name,
            write_disposition=write_disposition,
            table_name=f"{schema_name}.{table_name}",
            max_table_nesting=0
        )
        def load_table(config=table_config, load_type=load_type) -> Iterator[pa.Table]:
            """Load a single table based on its configuration"""
            
            if load_type == LoadType.INCREMENTAL:
                yield from pipeline.load_incremental(config)
            elif load_type == LoadType.FULL_REFRESH:
                yield from pipeline.load_full_refresh(config)
            elif load_type == LoadType.SCD:
                yield from pipeline.load_scd(config)
            else:
                raise ValueError(f"Unknown load type: {load_type}")
        
        yield load_table


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate the configuration file"""
    required_fields = ['prod_connection', 'staging_connection', 'tables']
    
    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required field in config: {field}")
            return False
    
    for table in config['tables']:
        if 'schema' not in table or 'table' not in table or 'load_type' not in table:
            logger.error(f"Table configuration missing required fields: {table}")
            return False
        
        load_type = table['load_type']
        if load_type == 'incremental' and 'watermark_column' not in table:
            logger.error(f"Incremental load requires watermark_column: {table}")
            return False
        
        if load_type == 'scd' and 'scd_config' not in table:
            logger.error(f"SCD load requires scd_config: {table}")
            return False
    
    return True


def run_pipeline(config_path: str = "pipeline_config.yml"):
    """
    Main function to run the DLT pipeline with configuration file
    """
    
    # Load configuration
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        raise
    
    # Validate configuration
    if not validate_config(config):
        raise ValueError("Invalid configuration file")
    
    # Get connection strings
    prod_conn_string = config['prod_connection']
    staging_conn_string = config['staging_connection']
    
    # Create pipeline instance
    pipeline_instance = MSSQLArrowPipeline(prod_conn_string, staging_conn_string)
    
    # Create DLT pipeline
    dlt_pipeline = dlt.pipeline(
        pipeline_name=config.get('pipeline_name', 'mssql_prod_to_staging'),
        destination=dlt.destinations.mssql(
            credentials=ConnectionStringCredentials(staging_conn_string),
            replace_strategy="staging-optimized"
        ),
        dataset_name=config.get('dataset_name', 'staging'),
        loader_file_format="parquet"  # Use Arrow/Parquet format
    )
    
    # Create source with all tables
    source = mssql_arrow_source(
        pipeline=pipeline_instance,
        table_configs=config['tables']
    )
    
    # Run the pipeline
    logger.info(f"Starting pipeline run at {datetime.now()}")
    info = dlt_pipeline.run(
        source,
        loader_file_format="parquet",
        write_disposition="append"  # Default, will be overridden per resource
    )
    
    # Log results
    logger.info(f"Pipeline completed at {datetime.now()}")
    logger.info(f"Load info: {info}")
    
    # Check for failures
    if info.has_failed_jobs:
        logger.error("Some jobs failed during the pipeline run")
        for job in info.load_packages[0].jobs.get("failed_jobs", []):
            logger.error(f"Failed job: {job}")
        raise RuntimeError("Pipeline execution failed with errors")
    
    return info


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run MSSQL to MSSQL DLT Pipeline')
    parser.add_argument(
        '--config', 
        type=str, 
        default='pipeline_config.yml',
        help='Path to configuration file (default: pipeline_config.yml)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        run_pipeline(args.config)
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

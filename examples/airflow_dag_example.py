"""Apache Airflow DAG example for the SQL Server Incremental Data Pipeline."""

from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.sensors.filesystem import FileSensor
    from airflow.utils.dates import days_ago
    from airflow.utils.task_group import TaskGroup
except ImportError:
    print("Apache Airflow not installed. This is just an example.")
    # Mock classes for demonstration
    class DAG: pass
    class PythonOperator: pass
    class BashOperator: pass
    class FileSensor: pass
    class TaskGroup: pass
    def days_ago(n): return datetime.now() - timedelta(days=n)

from src.config.models import (
    PipelineConfig, DatabaseConfig, ProcessingConfig, StateConfig,
    PartitionTableConfig, SCDTableConfig, StaticTableConfig
)
from src.pipeline.core import IncrementalSQLServerParquetPipeline


# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}


def create_pipeline_config() -> PipelineConfig:
    """Create pipeline configuration for Airflow execution."""
    
    database_config = DatabaseConfig(
        server="{{ var.value.sql_server_host }}",
        database="{{ var.value.sql_server_database }}",
        username="{{ var.value.sql_server_username }}",
        password="{{ var.value.sql_server_password }}",
        integrated_security=False,
        trust_server_certificate=True,
        connection_timeout=30,
        command_timeout=1800  # 30 minutes
    )
    
    processing_config = ProcessingConfig(
        batch_size=50000,
        max_workers=6,
        retry_attempts=3,
        enable_sql_server_partition_switching=True
    )
    
    state_config = StateConfig(
        state_directory=Path("{{ var.value.pipeline_state_dir }}"),
        parquet_directory=Path("{{ var.value.pipeline_data_dir }}"),
        enable_self_healing=True,
        state_retention_days=90
    )
    
    # Partition tables configuration
    partition_tables = [
        PartitionTableConfig(
            table_name="Orders",
            schema_name="sales",
            target_table="orders",
            target_schema="warehouse",
            partition_column="OrderDate",
            batch_size=100000
        ),
        PartitionTableConfig(
            table_name="OrderLineItems",
            schema_name="sales", 
            target_table="order_line_items",
            target_schema="warehouse",
            partition_column="OrderDate",
            batch_size=200000
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
    
    # SCD tables configuration
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
            end_date_column="ValidTo"
        ),
        SCDTableConfig(
            table_name="Products", 
            schema_name="catalog",
            target_table="products",
            target_schema="warehouse",
            business_key_columns=["ProductSKU"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsActive"
        )
    ]
    
    # Static tables configuration
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
        )
    ]
    
    return PipelineConfig(
        database=database_config,
        processing=processing_config,
        state=state_config,
        partition_tables=partition_tables,
        scd_tables=scd_tables,
        static_tables=static_tables,
        pipeline_name="warehouse_etl_pipeline",
        destination="parquet",
        log_level="INFO",
        enable_structured_logging=True,
        log_file=Path("{{ var.value.pipeline_log_dir }}/pipeline.log")
    )


def run_pipeline_validation(**context) -> bool:
    """Validate pipeline configuration and connectivity."""
    print("Starting pipeline validation...")
    
    config = create_pipeline_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Validate all tables
    validation_results = pipeline.validate_all_tables()
    
    print(f"Validation Results:")
    print(f"  Total tables: {validation_results['total_tables']}")
    print(f"  Valid tables: {validation_results['valid_tables']}")
    print(f"  Invalid tables: {validation_results['invalid_tables']}")
    
    # Push validation results to XCom for downstream tasks
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    if validation_results['invalid_tables'] > 0:
        print("Validation failed for some tables:")
        for table_validation in validation_results['table_validations']:
            if not table_validation['valid']:
                print(f"  ❌ {table_validation['table_name']}: {table_validation['issues']}")
        return False
    
    print("✅ All tables validated successfully")
    return True


def run_static_tables(**context) -> dict:
    """Run static reference tables."""
    print("Processing static reference tables...")
    
    config = create_pipeline_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Run only static tables
    results = pipeline.run(
        table_types=["static"],
        parallel_execution=True
    )
    
    print(f"Static tables processing completed:")
    print(f"  Tables processed: {results['tables_processed']}")
    print(f"  Total records: {results['total_records']}")
    print(f"  Duration: {results['total_duration_seconds']:.2f} seconds")
    
    # Push results to XCom
    context['task_instance'].xcom_push(key='static_results', value=results)
    
    return results


def run_scd_tables(**context) -> dict:
    """Run SCD Type 2 tables."""
    print("Processing SCD Type 2 tables...")
    
    config = create_pipeline_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Run only SCD tables
    results = pipeline.run(
        table_types=["scd"],
        parallel_execution=True
    )
    
    print(f"SCD tables processing completed:")
    print(f"  Tables processed: {results['tables_processed']}")
    print(f"  Total records: {results['total_records']}")
    print(f"  Duration: {results['total_duration_seconds']:.2f} seconds")
    
    # Push results to XCom
    context['task_instance'].xcom_push(key='scd_results', value=results)
    
    return results


def run_partition_tables(**context) -> dict:
    """Run partition tables."""
    print("Processing partition tables...")
    
    config = create_pipeline_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    # Run only partition tables
    results = pipeline.run(
        table_types=["partition"],
        parallel_execution=True
    )
    
    print(f"Partition tables processing completed:")
    print(f"  Tables processed: {results['tables_processed']}")
    print(f"  Total records: {results['total_records']}")
    print(f"  Duration: {results['total_duration_seconds']:.2f} seconds")
    
    # Check for any failures
    failed_tables = [r for r in results['table_results'] if not r['success']]
    if failed_tables:
        print("⚠️ Some partition tables failed:")
        for failed in failed_tables:
            print(f"  ❌ {failed['table_name']}: {failed['error']}")
    
    # Push results to XCom
    context['task_instance'].xcom_push(key='partition_results', value=results)
    
    return results


def create_pipeline_summary(**context) -> dict:
    """Create summary of all pipeline results."""
    print("Creating pipeline execution summary...")
    
    # Pull results from XCom
    ti = context['task_instance']
    static_results = ti.xcom_pull(key='static_results', task_ids='static_tables_group.run_static_tables')
    scd_results = ti.xcom_pull(key='scd_results', task_ids='scd_tables_group.run_scd_tables')  
    partition_results = ti.xcom_pull(key='partition_results', task_ids='partition_tables_group.run_partition_tables')
    
    # Combine results
    total_tables = 0
    total_records = 0
    total_duration = 0
    all_successful = True
    
    for results in [static_results, scd_results, partition_results]:
        if results:
            total_tables += results.get('tables_processed', 0)
            total_records += results.get('total_records', 0)
            total_duration += results.get('total_duration_seconds', 0)
            if results.get('failed_tables', 0) > 0:
                all_successful = False
    
    summary = {
        'total_tables_processed': total_tables,
        'total_records_processed': total_records,
        'total_duration_seconds': total_duration,
        'records_per_second': total_records / max(total_duration, 0.001),
        'all_successful': all_successful,
        'execution_date': context['ds'],
        'pipeline_run_id': context['run_id']
    }
    
    print(f"Pipeline Summary:")
    print(f"  Total tables: {summary['total_tables_processed']}")
    print(f"  Total records: {summary['total_records_processed']:,}")
    print(f"  Duration: {summary['total_duration_seconds']:.2f} seconds")
    print(f"  Rate: {summary['records_per_second']:.2f} records/second")
    print(f"  Success: {'✅' if summary['all_successful'] else '❌'}")
    
    # Push summary to XCom
    context['task_instance'].xcom_push(key='pipeline_summary', value=summary)
    
    return summary


# Create the DAG
dag = DAG(
    'sql_server_incremental_pipeline',
    default_args=default_args,
    description='SQL Server Incremental Data Pipeline using DLT',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['data-warehouse', 'incremental', 'sql-server', 'dlt']
)


# Pre-processing tasks
validate_pipeline = PythonOperator(
    task_id='validate_pipeline',
    python_callable=run_pipeline_validation,
    dag=dag,
    doc_md="""
    ### Validate Pipeline Configuration
    
    This task validates:
    - Database connectivity
    - Table existence and schemas
    - Column mappings and data types
    - Partition configurations
    - SCD integrity
    """
)

# Check if state directory exists
check_state_directory = BashOperator(
    task_id='check_state_directory',
    bash_command='mkdir -p {{ var.value.pipeline_state_dir }} && ls -la {{ var.value.pipeline_state_dir }}',
    dag=dag
)

# Static tables processing group
with TaskGroup("static_tables_group", dag=dag) as static_tables_group:
    run_static_tables_task = PythonOperator(
        task_id='run_static_tables',
        python_callable=run_static_tables,
        dag=dag,
        doc_md="""
        ### Process Static Reference Tables
        
        Processes all static reference tables using hash-based change detection.
        """
    )

# SCD tables processing group  
with TaskGroup("scd_tables_group", dag=dag) as scd_tables_group:
    run_scd_tables_task = PythonOperator(
        task_id='run_scd_tables',
        python_callable=run_scd_tables,
        dag=dag,
        doc_md="""
        ### Process SCD Type 2 Tables
        
        Processes all SCD Type 2 tables with proper history tracking.
        """
    )

# Partition tables processing group
with TaskGroup("partition_tables_group", dag=dag) as partition_tables_group:
    run_partition_tables_task = PythonOperator(
        task_id='run_partition_tables',
        python_callable=run_partition_tables,
        dag=dag,
        doc_md="""
        ### Process Partition Tables
        
        Processes all partition tables with SQL Server partition switching optimization.
        """
    )

# Post-processing tasks
create_summary = PythonOperator(
    task_id='create_summary',
    python_callable=create_pipeline_summary,
    dag=dag,
    trigger_rule='all_done',  # Run even if some upstream tasks fail
    doc_md="""
    ### Create Pipeline Summary
    
    Creates a comprehensive summary of the pipeline execution.
    """
)

# Cleanup task
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
        # Clean up temporary files older than 7 days
        find {{ var.value.pipeline_state_dir }} -name "*.tmp" -mtime +7 -delete
        find {{ var.value.pipeline_data_dir }} -name "*.staging" -mtime +1 -delete
        echo "Cleanup completed"
    ''',
    dag=dag,
    trigger_rule='all_done'
)

# Set up task dependencies
validate_pipeline >> check_state_directory

# Static tables can run immediately after validation
check_state_directory >> static_tables_group

# SCD tables depend on static tables (for reference data)
static_tables_group >> scd_tables_group

# Partition tables can run in parallel with SCD tables
check_state_directory >> partition_tables_group

# Summary depends on all processing groups
[static_tables_group, scd_tables_group, partition_tables_group] >> create_summary

# Cleanup runs after everything
create_summary >> cleanup_temp_files


# Optional: Create a separate DAG for validation only
validation_dag = DAG(
    'sql_server_pipeline_validation',
    default_args=default_args,
    description='Validate SQL Server Pipeline Configuration',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['validation', 'sql-server', 'dlt']
)

validation_only = PythonOperator(
    task_id='validate_configuration',
    python_callable=run_pipeline_validation,
    dag=validation_dag
)


# Optional: Create emergency recovery DAG
def repair_scd_integrity(**context):
    """Emergency task to repair SCD integrity issues."""
    config = create_pipeline_config()
    pipeline = IncrementalSQLServerParquetPipeline(config)
    
    repair_results = []
    
    for scd_config in config.scd_tables:
        print(f"Repairing SCD integrity for {scd_config.table_name}...")
        
        repair_result = pipeline.scd_handler.repair_scd_integrity(
            scd_config, 
            dry_run=False  # Actually perform repairs
        )
        
        repair_results.append({
            'table': f"{scd_config.schema_name}.{scd_config.table_name}",
            'violations_found': repair_result['violations_found'],
            'violations_repaired': repair_result['violations_repaired']
        })
    
    print("SCD integrity repair completed:")
    for result in repair_results:
        print(f"  {result['table']}: {result['violations_repaired']}/{result['violations_found']} repaired")
    
    return repair_results


recovery_dag = DAG(
    'sql_server_pipeline_recovery',
    default_args=default_args,
    description='Emergency recovery tasks for SQL Server Pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['recovery', 'maintenance', 'sql-server']
)

repair_scd_task = PythonOperator(
    task_id='repair_scd_integrity',
    python_callable=repair_scd_integrity,
    dag=recovery_dag
)


if __name__ == "__main__":
    """Print DAG information when run directly."""
    print("SQL Server Incremental Pipeline DAGs:")
    print(f"  Main DAG: {dag.dag_id}")
    print(f"  Schedule: {dag.schedule_interval}")
    print(f"  Tasks: {len(dag.task_dict)}")
    print(f"  Validation DAG: {validation_dag.dag_id}")
    print(f"  Recovery DAG: {recovery_dag.dag_id}")
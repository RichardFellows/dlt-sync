"""Self-healing state detection and recovery for the incremental pipeline."""

from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json
import pickle
import hashlib
import os
import glob
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import structlog

from ..config.models import StateConfig, PartitionTableConfig, SCDTableConfig, StaticTableConfig
from .sql_utils import SQLServerUtils

logger = structlog.get_logger(__name__)


class PipelineState:
    """Represents the state of a single table in the pipeline."""
    
    def __init__(
        self,
        table_name: str,
        table_type: str,
        last_updated: Optional[datetime] = None,
        last_processed_value: Optional[Any] = None,
        last_hash: Optional[str] = None,
        records_processed: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize pipeline state.
        
        Args:
            table_name: Full table name (schema.table)
            table_type: Table type (partition, scd, static)
            last_updated: When state was last updated
            last_processed_value: Last processed partition/timestamp value
            last_hash: Last computed hash (for static tables)
            records_processed: Number of records processed
            metadata: Additional metadata
        """
        self.table_name = table_name
        self.table_type = table_type
        self.last_updated = last_updated or datetime.utcnow()
        self.last_processed_value = last_processed_value
        self.last_hash = last_hash
        self.records_processed = records_processed
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary."""
        return {
            "table_name": self.table_name,
            "table_type": self.table_type,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "last_processed_value": self.last_processed_value,
            "last_hash": self.last_hash,
            "records_processed": self.records_processed,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineState":
        """Create state from dictionary."""
        last_updated = None
        if data.get("last_updated"):
            try:
                last_updated = datetime.fromisoformat(data["last_updated"])
            except (ValueError, TypeError):
                pass
        
        return cls(
            table_name=data["table_name"],
            table_type=data["table_type"],
            last_updated=last_updated,
            last_processed_value=data.get("last_processed_value"),
            last_hash=data.get("last_hash"),
            records_processed=data.get("records_processed", 0),
            metadata=data.get("metadata", {})
        )


class StateManager:
    """Manages pipeline state with self-healing capabilities."""
    
    def __init__(
        self,
        config: StateConfig,
        sql_utils: SQLServerUtils,
        pipeline_name: str = "sql_server_incremental"
    ):
        """
        Initialize state manager.
        
        Args:
            config: State configuration
            sql_utils: SQL Server utilities
            pipeline_name: Pipeline name
        """
        self.config = config
        self.sql_utils = sql_utils
        self.pipeline_name = pipeline_name
        
        # Ensure directories exist
        self.config.state_directory.mkdir(parents=True, exist_ok=True)
        self.config.parquet_directory.mkdir(parents=True, exist_ok=True)
        
        self.state_file = self.config.state_directory / f"{pipeline_name}_state.json"
        self.backup_pattern = f"{pipeline_name}_state_backup_*.json"
        
        self._current_states: Dict[str, PipelineState] = {}
        self._load_states()
    
    def _load_states(self) -> None:
        """Load pipeline states from file."""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                for table_name, data in state_data.items():
                    self._current_states[table_name] = PipelineState.from_dict(data)
                
                logger.info("Loaded pipeline states", count=len(self._current_states))
            else:
                logger.info("No existing state file found, starting fresh")
                
        except Exception as e:
            logger.error("Failed to load pipeline states", error=str(e))
            # Try to recover from backup
            self._recover_from_backup()
    
    def _save_states(self) -> None:
        """Save pipeline states to file."""
        try:
            # Backup existing state if it exists
            if self.config.backup_state_before_run and self.state_file.exists():
                self._backup_current_state()
            
            # Save current states
            state_data = {}
            for table_name, state in self._current_states.items():
                state_data[table_name] = state.to_dict()
            
            # Write atomically using temporary file
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, default=str)
            
            temp_file.replace(self.state_file)
            
            logger.debug("Saved pipeline states", count=len(self._current_states))
            
        except Exception as e:
            logger.error("Failed to save pipeline states", error=str(e))
            raise
    
    def _backup_current_state(self) -> None:
        """Create backup of current state file."""
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_file = self.config.state_directory / f"{self.pipeline_name}_state_backup_{timestamp}.json"
            shutil.copy2(self.state_file, backup_file)
            
            logger.debug("Created state backup", backup_file=str(backup_file))
            
        except Exception as e:
            logger.warning("Failed to create state backup", error=str(e))
    
    def _recover_from_backup(self) -> None:
        """Attempt to recover state from most recent backup."""
        try:
            backup_files = glob.glob(
                str(self.config.state_directory / self.backup_pattern)
            )
            
            if not backup_files:
                logger.info("No backup files found for recovery")
                return
            
            # Get most recent backup
            latest_backup = max(backup_files, key=os.path.getctime)
            
            with open(latest_backup, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            for table_name, data in state_data.items():
                self._current_states[table_name] = PipelineState.from_dict(data)
            
            logger.info(
                "Recovered state from backup",
                backup_file=latest_backup,
                states_recovered=len(self._current_states)
            )
            
        except Exception as e:
            logger.error("Failed to recover from backup", error=str(e))
    
    def get_state(self, table_name: str) -> Optional[PipelineState]:
        """
        Get current state for a table.
        
        Args:
            table_name: Full table name (schema.table)
            
        Returns:
            Pipeline state or None if not found
        """
        return self._current_states.get(table_name)
    
    def update_state(self, state: PipelineState) -> None:
        """
        Update state for a table.
        
        Args:
            state: Updated pipeline state
        """
        self._current_states[state.table_name] = state
        state.last_updated = datetime.utcnow()
        
        # Save immediately for durability
        self._save_states()
        
        logger.debug(
            "Updated pipeline state",
            table_name=state.table_name,
            table_type=state.table_type,
            records_processed=state.records_processed
        )
    
    def detect_and_recover_partition_state(
        self, 
        config: PartitionTableConfig
    ) -> PipelineState:
        """
        Detect and recover state for partition table using self-healing logic.
        
        Args:
            config: Partition table configuration
            
        Returns:
            Recovered or detected pipeline state
        """
        table_name = f"{config.schema_name}.{config.table_name}"
        
        # Get current state if exists
        current_state = self.get_state(table_name)
        
        # Detect state from Parquet files
        parquet_state = self._detect_state_from_parquet_files(table_name, "partition")
        
        # Detect state from target database
        target_state = self._detect_state_from_target_partition_table(config)
        
        # Self-healing logic: use minimum of detected states for safety
        recovered_state = self._reconcile_partition_states(
            current_state, parquet_state, target_state, config
        )
        
        logger.info(
            "Recovered partition table state",
            table_name=table_name,
            current_value=current_state.last_processed_value if current_state else None,
            parquet_value=parquet_state.last_processed_value if parquet_state else None,
            target_value=target_state.last_processed_value if target_state else None,
            recovered_value=recovered_state.last_processed_value
        )
        
        return recovered_state
    
    def detect_and_recover_scd_state(
        self,
        config: SCDTableConfig
    ) -> PipelineState:
        """
        Detect and recover state for SCD Type 2 table.
        
        Args:
            config: SCD table configuration
            
        Returns:
            Recovered or detected pipeline state
        """
        table_name = f"{config.schema_name}.{config.table_name}"
        
        # Get current state if exists
        current_state = self.get_state(table_name)
        
        # Detect state from Parquet files
        parquet_state = self._detect_state_from_parquet_files(table_name, "scd")
        
        # Detect state from target database
        target_state = self._detect_state_from_target_scd_table(config)
        
        # Self-healing logic: use minimum timestamp for safety
        recovered_state = self._reconcile_scd_states(
            current_state, parquet_state, target_state, config
        )
        
        logger.info(
            "Recovered SCD table state",
            table_name=table_name,
            current_timestamp=current_state.last_processed_value if current_state else None,
            parquet_timestamp=parquet_state.last_processed_value if parquet_state else None,
            target_timestamp=target_state.last_processed_value if target_state else None,
            recovered_timestamp=recovered_state.last_processed_value
        )
        
        return recovered_state
    
    def detect_and_recover_static_state(
        self,
        config: StaticTableConfig
    ) -> PipelineState:
        """
        Detect and recover state for static reference table.
        
        Args:
            config: Static table configuration
            
        Returns:
            Recovered or detected pipeline state
        """
        table_name = f"{config.schema_name}.{config.table_name}"
        
        # Get current state if exists
        current_state = self.get_state(table_name)
        
        # Compute current hash from source table
        try:
            current_hash = self.sql_utils.get_table_hash(
                config.schema_name,
                config.table_name,
                config.hash_algorithm
            )
        except Exception as e:
            logger.error(
                "Failed to compute current table hash",
                table_name=table_name,
                error=str(e)
            )
            current_hash = None
        
        # Check if reload is needed
        needs_reload = self._check_static_table_needs_reload(current_state, current_hash, config)
        
        if current_state and not needs_reload:
            return current_state
        
        # Create new state
        recovered_state = PipelineState(
            table_name=table_name,
            table_type="static",
            last_hash=current_hash,
            metadata={
                "hash_algorithm": config.hash_algorithm,
                "load_strategy": config.load_strategy,
                "needs_reload": needs_reload,
                "reload_reason": "hash_changed" if current_state else "initial_load"
            }
        )
        
        logger.info(
            "Recovered static table state",
            table_name=table_name,
            current_hash=current_hash[:16] + "..." if current_hash else None,
            needs_reload=needs_reload
        )
        
        return recovered_state
    
    def _detect_state_from_parquet_files(
        self,
        table_name: str,
        table_type: str
    ) -> Optional[PipelineState]:
        """Detect state from existing Parquet files."""
        try:
            # Look for Parquet files for this table
            safe_table_name = table_name.replace(".", "_")
            parquet_pattern = self.config.parquet_directory / f"{safe_table_name}*.parquet"
            parquet_files = glob.glob(str(parquet_pattern))
            
            if not parquet_files:
                return None
            
            # Get most recent file
            latest_file = max(parquet_files, key=os.path.getctime)
            
            # Read metadata from Parquet file
            parquet_file = pq.ParquetFile(latest_file)
            metadata = parquet_file.metadata_path
            
            if metadata and hasattr(metadata, 'metadata'):
                file_metadata = metadata.metadata
                
                last_value = None
                if b'last_processed_value' in file_metadata:
                    last_value = file_metadata[b'last_processed_value'].decode('utf-8')
                
                last_hash = None
                if b'table_hash' in file_metadata:
                    last_hash = file_metadata[b'table_hash'].decode('utf-8')
                
                return PipelineState(
                    table_name=table_name,
                    table_type=table_type,
                    last_updated=datetime.fromtimestamp(os.path.getctime(latest_file)),
                    last_processed_value=last_value,
                    last_hash=last_hash,
                    metadata={"source": "parquet_file", "file_path": latest_file}
                )
            
        except Exception as e:
            logger.debug(
                "Could not detect state from Parquet files",
                table_name=table_name,
                error=str(e)
            )
        
        return None
    
    def _detect_state_from_target_partition_table(
        self,
        config: PartitionTableConfig
    ) -> Optional[PipelineState]:
        """Detect state from target partition table."""
        try:
            # Get distinct partition values from target
            distinct_values = self.sql_utils.get_distinct_partition_values(
                config.target_schema,
                config.target_table,
                config.partition_column
            )
            
            if distinct_values:
                max_value = max(distinct_values)
                return PipelineState(
                    table_name=f"{config.schema_name}.{config.table_name}",
                    table_type="partition",
                    last_processed_value=max_value,
                    metadata={"source": "target_table", "distinct_values_count": len(distinct_values)}
                )
        
        except Exception as e:
            logger.debug(
                "Could not detect state from target partition table",
                table_name=f"{config.schema_name}.{config.table_name}",
                error=str(e)
            )
        
        return None
    
    def _detect_state_from_target_scd_table(
        self,
        config: SCDTableConfig
    ) -> Optional[PipelineState]:
        """Detect state from target SCD table."""
        try:
            # Get maximum last modified timestamp from target
            min_ts, max_ts = self.sql_utils.get_min_max_timestamp(
                config.target_schema,
                config.target_table,
                config.last_modified_column
            )
            
            if max_ts:
                return PipelineState(
                    table_name=f"{config.schema_name}.{config.table_name}",
                    table_type="scd",
                    last_processed_value=max_ts,
                    metadata={
                        "source": "target_table",
                        "min_timestamp": min_ts,
                        "max_timestamp": max_ts
                    }
                )
        
        except Exception as e:
            logger.debug(
                "Could not detect state from target SCD table",
                table_name=f"{config.schema_name}.{config.table_name}",
                error=str(e)
            )
        
        return None
    
    def _reconcile_partition_states(
        self,
        current: Optional[PipelineState],
        parquet: Optional[PipelineState],
        target: Optional[PipelineState],
        config: PartitionTableConfig
    ) -> PipelineState:
        """Reconcile partition states using self-healing logic."""
        table_name = f"{config.schema_name}.{config.table_name}"
        
        # Collect all available values
        available_values = []
        
        if current and current.last_processed_value is not None:
            available_values.append(("current", current.last_processed_value))
        
        if parquet and parquet.last_processed_value is not None:
            available_values.append(("parquet", parquet.last_processed_value))
        
        if target and target.last_processed_value is not None:
            available_values.append(("target", target.last_processed_value))
        
        if not available_values:
            # No previous state found, start from beginning
            return PipelineState(
                table_name=table_name,
                table_type="partition",
                metadata={"recovery_reason": "no_previous_state"}
            )
        
        # Use minimum value for safety (conservative approach)
        min_source, min_value = min(available_values, key=lambda x: x[1])
        
        return PipelineState(
            table_name=table_name,
            table_type="partition",
            last_processed_value=min_value,
            metadata={
                "recovery_reason": "reconciled_states",
                "recovered_from": min_source,
                "available_sources": [source for source, _ in available_values]
            }
        )
    
    def _reconcile_scd_states(
        self,
        current: Optional[PipelineState],
        parquet: Optional[PipelineState],
        target: Optional[PipelineState],
        config: SCDTableConfig
    ) -> PipelineState:
        """Reconcile SCD states using self-healing logic."""
        table_name = f"{config.schema_name}.{config.table_name}"
        
        # Collect all available timestamps
        available_timestamps = []
        
        if current and current.last_processed_value is not None:
            if isinstance(current.last_processed_value, str):
                try:
                    ts = datetime.fromisoformat(current.last_processed_value)
                    available_timestamps.append(("current", ts))
                except ValueError:
                    pass
            elif isinstance(current.last_processed_value, datetime):
                available_timestamps.append(("current", current.last_processed_value))
        
        if parquet and parquet.last_processed_value is not None:
            if isinstance(parquet.last_processed_value, str):
                try:
                    ts = datetime.fromisoformat(parquet.last_processed_value)
                    available_timestamps.append(("parquet", ts))
                except ValueError:
                    pass
            elif isinstance(parquet.last_processed_value, datetime):
                available_timestamps.append(("parquet", parquet.last_processed_value))
        
        if target and target.last_processed_value is not None:
            if isinstance(target.last_processed_value, datetime):
                available_timestamps.append(("target", target.last_processed_value))
        
        if not available_timestamps:
            # No previous state found, start from beginning
            return PipelineState(
                table_name=table_name,
                table_type="scd",
                metadata={"recovery_reason": "no_previous_state"}
            )
        
        # Use minimum timestamp for safety (conservative approach)
        min_source, min_timestamp = min(available_timestamps, key=lambda x: x[1])
        
        return PipelineState(
            table_name=table_name,
            table_type="scd",
            last_processed_value=min_timestamp,
            metadata={
                "recovery_reason": "reconciled_states",
                "recovered_from": min_source,
                "available_sources": [source for source, _ in available_timestamps]
            }
        )
    
    def _check_static_table_needs_reload(
        self,
        current_state: Optional[PipelineState],
        current_hash: Optional[str],
        config: StaticTableConfig
    ) -> bool:
        """Check if static table needs reload."""
        if not current_state:
            return True
        
        # Check if hash changed
        if current_hash != current_state.last_hash:
            return True
        
        # Check if forced reload is needed based on age
        if (config.force_reload_after_days and current_state.last_updated and
            datetime.utcnow() - current_state.last_updated > timedelta(days=config.force_reload_after_days)):
            return True
        
        return False
    
    def cleanup_old_state_backups(self, retention_days: Optional[int] = None) -> int:
        """
        Clean up old state backup files.
        
        Args:
            retention_days: Days to retain (uses config default if None)
            
        Returns:
            Number of files cleaned up
        """
        if retention_days is None:
            retention_days = self.config.state_retention_days
        
        cutoff_time = datetime.utcnow() - timedelta(days=retention_days)
        
        try:
            backup_files = glob.glob(
                str(self.config.state_directory / self.backup_pattern)
            )
            
            cleaned_count = 0
            for backup_file in backup_files:
                if datetime.fromtimestamp(os.path.getctime(backup_file)) < cutoff_time:
                    os.remove(backup_file)
                    cleaned_count += 1
            
            logger.info(
                "Cleaned up old state backups",
                cleaned_count=cleaned_count,
                retention_days=retention_days
            )
            
            return cleaned_count
            
        except Exception as e:
            logger.error("Failed to cleanup old state backups", error=str(e))
            return 0
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline status.
        
        Returns:
            Dictionary with pipeline status information
        """
        status = {
            "pipeline_name": self.pipeline_name,
            "state_file": str(self.state_file),
            "state_exists": self.state_file.exists(),
            "total_tables": len(self._current_states),
            "tables": {},
            "last_backup_time": None,
            "state_directory_size_mb": 0
        }
        
        # Get table statuses
        for table_name, state in self._current_states.items():
            status["tables"][table_name] = {
                "table_type": state.table_type,
                "last_updated": state.last_updated.isoformat() if state.last_updated else None,
                "last_processed_value": str(state.last_processed_value) if state.last_processed_value else None,
                "records_processed": state.records_processed,
                "has_hash": state.last_hash is not None,
                "metadata_keys": list(state.metadata.keys()) if state.metadata else []
            }
        
        # Get backup information
        try:
            backup_files = glob.glob(
                str(self.config.state_directory / self.backup_pattern)
            )
            if backup_files:
                latest_backup = max(backup_files, key=os.path.getctime)
                status["last_backup_time"] = datetime.fromtimestamp(
                    os.path.getctime(latest_backup)
                ).isoformat()
        except Exception:
            pass
        
        # Calculate directory size
        try:
            total_size = sum(
                f.stat().st_size 
                for f in self.config.state_directory.rglob('*') 
                if f.is_file()
            )
            status["state_directory_size_mb"] = round(total_size / (1024 * 1024), 2)
        except Exception:
            pass
        
        return status
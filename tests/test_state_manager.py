"""Tests for state manager."""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import json
from unittest.mock import Mock, patch

from src.pipeline.state_manager import StateManager, PipelineState
from src.config.models import StateConfig, PartitionTableConfig, SCDTableConfig, StaticTableConfig


class TestPipelineState:
    """Test pipeline state functionality."""
    
    def test_pipeline_state_creation(self):
        """Test creating a pipeline state."""
        state = PipelineState(
            table_name="test.table",
            table_type="partition",
            last_processed_value=100,
            records_processed=1000
        )
        
        assert state.table_name == "test.table"
        assert state.table_type == "partition"
        assert state.last_processed_value == 100
        assert state.records_processed == 1000
        assert state.last_updated is not None
        
    def test_pipeline_state_to_dict(self):
        """Test converting pipeline state to dictionary."""
        now = datetime.utcnow()
        state = PipelineState(
            table_name="test.table",
            table_type="scd",
            last_updated=now,
            last_processed_value=now,
            records_processed=500,
            metadata={"test": "value"}
        )
        
        state_dict = state.to_dict()
        
        assert state_dict["table_name"] == "test.table"
        assert state_dict["table_type"] == "scd"
        assert state_dict["last_updated"] == now.isoformat()
        assert state_dict["records_processed"] == 500
        assert state_dict["metadata"]["test"] == "value"
        
    def test_pipeline_state_from_dict(self):
        """Test creating pipeline state from dictionary."""
        now = datetime.utcnow()
        state_dict = {
            "table_name": "test.table",
            "table_type": "static",
            "last_updated": now.isoformat(),
            "last_processed_value": "test_hash",
            "last_hash": "abc123",
            "records_processed": 750,
            "metadata": {"hash_algorithm": "sha256"}
        }
        
        state = PipelineState.from_dict(state_dict)
        
        assert state.table_name == "test.table"
        assert state.table_type == "static"
        assert abs((state.last_updated - now).total_seconds()) < 1
        assert state.last_processed_value == "test_hash"
        assert state.last_hash == "abc123"
        assert state.records_processed == 750
        assert state.metadata["hash_algorithm"] == "sha256"


class TestStateManager:
    """Test state manager functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.state_config = StateConfig(
            state_directory=self.temp_dir / "state",
            parquet_directory=self.temp_dir / "data",
            enable_self_healing=True,
            state_retention_days=7
        )
        
        # Mock SQL utilities
        self.mock_sql_utils = Mock()
        self.mock_sql_utils.get_distinct_partition_values.return_value = [1, 2, 3]
        self.mock_sql_utils.get_min_max_timestamp.return_value = (
            datetime(2023, 1, 1), 
            datetime(2023, 1, 31)
        )
        self.mock_sql_utils.get_table_hash.return_value = "abc123"
        
        self.state_manager = StateManager(
            self.state_config,
            self.mock_sql_utils,
            "test_pipeline"
        )
        
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_state_manager_initialization(self):
        """Test state manager initialization."""
        assert self.state_manager.pipeline_name == "test_pipeline"
        assert self.state_manager.config.state_directory.exists()
        assert self.state_manager.config.parquet_directory.exists()
        assert len(self.state_manager._current_states) == 0
    
    def test_update_and_get_state(self):
        """Test updating and retrieving state."""
        state = PipelineState(
            table_name="test.table",
            table_type="partition",
            last_processed_value=100,
            records_processed=1000
        )
        
        # Update state
        self.state_manager.update_state(state)
        
        # Retrieve state
        retrieved_state = self.state_manager.get_state("test.table")
        
        assert retrieved_state is not None
        assert retrieved_state.table_name == "test.table"
        assert retrieved_state.last_processed_value == 100
        assert retrieved_state.records_processed == 1000
    
    def test_get_nonexistent_state(self):
        """Test getting state for nonexistent table."""
        state = self.state_manager.get_state("nonexistent.table")
        assert state is None
    
    def test_state_persistence(self):
        """Test state persistence to file."""
        # Create and update state
        state = PipelineState(
            table_name="test.table",
            table_type="partition",
            last_processed_value=100
        )
        
        self.state_manager.update_state(state)
        
        # Create new state manager to test loading
        new_state_manager = StateManager(
            self.state_config,
            self.mock_sql_utils,
            "test_pipeline"
        )
        
        # Should load the saved state
        retrieved_state = new_state_manager.get_state("test.table")
        assert retrieved_state is not None
        assert retrieved_state.table_name == "test.table"
        assert retrieved_state.last_processed_value == 100
    
    def test_detect_partition_state_no_existing_state(self):
        """Test detecting partition state with no existing state."""
        config = PartitionTableConfig(
            table_name="TestTable",
            schema_name="test",
            target_table="test_target",
            target_schema="target",
            partition_column="DateCol"
        )
        
        # Mock target state detection to return some values
        self.mock_sql_utils.get_distinct_partition_values.return_value = [1, 2, 3]
        
        state = self.state_manager.detect_and_recover_partition_state(config)
        
        assert state.table_name == "test.TestTable"
        assert state.table_type == "partition"
        # Should use conservative approach (minimum value)
        assert state.last_processed_value in [None, 1]  # Depends on reconciliation logic
    
    def test_detect_scd_state_no_existing_state(self):
        """Test detecting SCD state with no existing state."""
        config = SCDTableConfig(
            table_name="TestSCD",
            schema_name="test",
            target_table="test_target",
            target_schema="target",
            business_key_columns=["ID"],
            last_modified_column="ModifiedDate",
            is_latest_column="IsLatest"
        )
        
        # Mock timestamp detection
        min_ts = datetime(2023, 1, 1)
        max_ts = datetime(2023, 1, 31)
        self.mock_sql_utils.get_min_max_timestamp.return_value = (min_ts, max_ts)
        
        state = self.state_manager.detect_and_recover_scd_state(config)
        
        assert state.table_name == "test.TestSCD"
        assert state.table_type == "scd"
        # Should start from minimum timestamp for first run
        assert state.last_processed_value in [None, min_ts]
    
    def test_detect_static_state_no_existing_state(self):
        """Test detecting static state with no existing state.""" 
        config = StaticTableConfig(
            table_name="TestStatic",
            schema_name="test",
            target_table="test_target",
            target_schema="target",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace"
        )
        
        # Mock hash computation
        self.mock_sql_utils.get_table_hash.return_value = "new_hash_123"
        
        state = self.state_manager.detect_and_recover_static_state(config)
        
        assert state.table_name == "test.TestStatic"
        assert state.table_type == "static"
        assert state.last_hash == "new_hash_123"
        assert state.metadata["needs_reload"] == True
    
    def test_detect_static_state_unchanged_hash(self):
        """Test detecting static state with unchanged hash."""
        config = StaticTableConfig(
            table_name="TestStatic",
            schema_name="test", 
            target_table="test_target",
            target_schema="target",
            hash_algorithm="hashbytes_sha256",
            load_strategy="replace"
        )
        
        # Create existing state with same hash
        existing_hash = "same_hash_123"
        existing_state = PipelineState(
            table_name="test.TestStatic",
            table_type="static",
            last_hash=existing_hash,
            last_updated=datetime.utcnow() - timedelta(hours=1)
        )
        
        self.state_manager.update_state(existing_state)
        
        # Mock hash computation to return same hash
        self.mock_sql_utils.get_table_hash.return_value = existing_hash
        
        state = self.state_manager.detect_and_recover_static_state(config)
        
        # Should return existing state since hash hasn't changed
        assert state.table_name == "test.TestStatic"
        assert state.last_hash == existing_hash
        assert state.metadata.get("needs_reload") == False
    
    def test_reconcile_partition_states(self):
        """Test reconciliation of partition states."""
        config = PartitionTableConfig(
            table_name="TestTable",
            schema_name="test",
            target_table="test_target", 
            target_schema="target",
            partition_column="DateCol"
        )
        
        # Create different states with different values
        current_state = PipelineState(
            table_name="test.TestTable",
            table_type="partition",
            last_processed_value=10  # Higher value
        )
        
        # Mock parquet state (lower value)
        with patch.object(self.state_manager, '_detect_state_from_parquet_files') as mock_parquet:
            mock_parquet.return_value = PipelineState(
                table_name="test.TestTable", 
                table_type="partition",
                last_processed_value=5  # Lower value
            )
            
            # Mock target state (middle value)
            with patch.object(self.state_manager, '_detect_state_from_target_partition_table') as mock_target:
                mock_target.return_value = PipelineState(
                    table_name="test.TestTable",
                    table_type="partition", 
                    last_processed_value=7  # Middle value
                )
                
                # Update current state first
                self.state_manager.update_state(current_state)
                
                # Should reconcile to minimum value (5) for safety
                reconciled_state = self.state_manager.detect_and_recover_partition_state(config)
                assert reconciled_state.last_processed_value == 5
    
    def test_get_pipeline_status(self):
        """Test getting pipeline status."""
        # Add some states
        state1 = PipelineState(
            table_name="test.table1",
            table_type="partition",
            records_processed=1000
        )
        
        state2 = PipelineState(
            table_name="test.table2", 
            table_type="scd",
            records_processed=500
        )
        
        self.state_manager.update_state(state1)
        self.state_manager.update_state(state2)
        
        status = self.state_manager.get_pipeline_status()
        
        assert status["pipeline_name"] == "test_pipeline"
        assert status["total_tables"] == 2
        assert "test.table1" in status["tables"]
        assert "test.table2" in status["tables"]
        assert status["tables"]["test.table1"]["table_type"] == "partition"
        assert status["tables"]["test.table2"]["table_type"] == "scd"
    
    @patch('glob.glob')
    @patch('os.path.getctime')
    @patch('os.remove')
    def test_cleanup_old_state_backups(self, mock_remove, mock_getctime, mock_glob):
        """Test cleanup of old state backup files."""
        # Mock finding old backup files
        old_file = str(self.temp_dir / "old_backup.json")
        recent_file = str(self.temp_dir / "recent_backup.json")
        
        mock_glob.return_value = [old_file, recent_file]
        
        # Mock file creation times
        old_time = (datetime.utcnow() - timedelta(days=10)).timestamp()
        recent_time = (datetime.utcnow() - timedelta(hours=1)).timestamp()
        
        def getctime_side_effect(file_path):
            if file_path == old_file:
                return old_time
            return recent_time
        
        mock_getctime.side_effect = getctime_side_effect
        
        # Should clean up old file but not recent one
        cleaned_count = self.state_manager.cleanup_old_state_backups(retention_days=7)
        
        assert cleaned_count == 1
        mock_remove.assert_called_once_with(old_file)


if __name__ == "__main__":
    pytest.main([__file__])
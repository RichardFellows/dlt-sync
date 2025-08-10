"""Tests for decimal handler."""

import pytest
from decimal import Decimal
import pyarrow as pa
import pandas as pd

from src.pipeline.decimal_handler import DecimalHandler


class TestDecimalHandler:
    """Test decimal handler functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.handler = DecimalHandler()
    
    def test_sql_type_to_arrow_type_decimal(self):
        """Test conversion of SQL decimal types to Arrow types."""
        arrow_type = self.handler.sql_type_to_arrow_type("decimal", 18, 4)
        
        assert pa.types.is_decimal(arrow_type)
        assert arrow_type.precision == 18
        assert arrow_type.scale == 4
        
    def test_sql_type_to_arrow_type_money(self):
        """Test conversion of SQL money type to Arrow type."""
        arrow_type = self.handler.sql_type_to_arrow_type("money")
        
        assert pa.types.is_decimal(arrow_type)
        assert arrow_type.precision == 19
        assert arrow_type.scale == 4
        
    def test_sql_type_to_arrow_type_int(self):
        """Test conversion of SQL integer type to Arrow type."""
        arrow_type = self.handler.sql_type_to_arrow_type("int")
        
        assert pa.types.is_integer(arrow_type)
        assert arrow_type == pa.int32()
        
    def test_sql_type_to_arrow_type_unsupported(self):
        """Test unsupported SQL type."""
        with pytest.raises(ValueError, match="Unsupported SQL Server type"):
            self.handler.sql_type_to_arrow_type("unsupported_type")
            
    def test_sql_type_to_arrow_type_missing_precision(self):
        """Test decimal type without precision."""
        with pytest.raises(ValueError, match="Precision and scale required"):
            self.handler.sql_type_to_arrow_type("decimal")
            
    def test_sql_type_to_arrow_type_excessive_precision(self):
        """Test decimal type with excessive precision."""
        with pytest.raises(ValueError, match="Precision .* exceeds Arrow decimal128 limit"):
            self.handler.sql_type_to_arrow_type("decimal", 50, 2)
    
    def test_create_arrow_schema_from_sql_schema(self):
        """Test creating Arrow schema from SQL schema info."""
        sql_schema = {
            "ID": {
                "data_type": "int",
                "is_nullable": False,
                "precision": None,
                "scale": None
            },
            "Amount": {
                "data_type": "decimal",
                "is_nullable": True,
                "precision": 18,
                "scale": 4
            },
            "Name": {
                "data_type": "varchar",
                "is_nullable": True,
                "precision": None,
                "scale": None
            }
        }
        
        # We need to add varchar to type mappings for this test
        self.handler.type_mappings["varchar"] = {"arrow_type": pa.string, "requires_precision": False}
        
        arrow_schema = self.handler.create_arrow_schema_from_sql_schema(sql_schema)
        
        assert len(arrow_schema) == 3
        
        # Check ID field
        id_field = arrow_schema.field("ID")
        assert id_field.type == pa.int32()
        assert not id_field.nullable
        
        # Check Amount field  
        amount_field = arrow_schema.field("Amount")
        assert pa.types.is_decimal(amount_field.type)
        assert amount_field.type.precision == 18
        assert amount_field.type.scale == 4
        assert amount_field.nullable
        
        # Check Name field
        name_field = arrow_schema.field("Name")
        assert name_field.type == pa.string()
        assert name_field.nullable
    
    def test_validate_decimal_precision_in_table(self):
        """Test decimal precision validation in Arrow table."""
        # Create test data
        schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("amount", pa.decimal128(18, 4))
        ])
        
        data = [
            {"id": 1, "amount": Decimal("100.5000")},
            {"id": 2, "amount": Decimal("200.2500")}
        ]
        
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=schema)
        
        sql_schema = {
            "id": {"data_type": "int"},
            "amount": {"data_type": "decimal", "precision": 18, "scale": 4}
        }
        
        issues = self.handler.validate_decimal_precision_in_table(table, sql_schema)
        
        assert len(issues) == 0  # No issues expected
        
    def test_validate_decimal_precision_mismatch(self):
        """Test decimal precision validation with mismatch."""
        # Create test data with wrong precision
        schema = pa.schema([
            pa.field("amount", pa.decimal128(10, 2))  # Wrong precision
        ])
        
        data = [{"amount": Decimal("100.50")}]
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=schema)
        
        sql_schema = {
            "amount": {"data_type": "decimal", "precision": 18, "scale": 4}  # Expected precision
        }
        
        issues = self.handler.validate_decimal_precision_in_table(table, sql_schema)
        
        assert len(issues) == 2  # Precision and scale mismatch
        assert any("Precision mismatch" in issue["issue"] for issue in issues)
        assert any("Scale mismatch" in issue["issue"] for issue in issues)
    
    def test_format_decimal_for_sql_server(self):
        """Test formatting decimal for SQL Server."""
        # Test normal decimal
        result = self.handler.format_decimal_for_sql_server(Decimal("123.45"), 10, 2)
        assert result == "123.45"
        
        # Test None value
        result = self.handler.format_decimal_for_sql_server(None, 10, 2)
        assert result is None
        
        # Test string input
        result = self.handler.format_decimal_for_sql_server("456.78", 10, 2)
        assert result == "456.78"
        
        # Test float input
        result = self.handler.format_decimal_for_sql_server(789.12, 10, 2)
        assert result == "789.12"
    
    def test_extract_decimal_from_arrow_array(self):
        """Test extracting decimals from Arrow array."""
        # Create decimal array
        decimals = [Decimal("123.45"), None, Decimal("678.90")]
        array = pa.array(decimals, type=pa.decimal128(10, 2))
        
        extracted = self.handler.extract_decimal_from_arrow_array(array)
        
        assert len(extracted) == 3
        assert extracted[0] == Decimal("123.45")
        assert extracted[1] is None
        assert extracted[2] == Decimal("678.90")
    
    def test_extract_decimal_non_decimal_array(self):
        """Test extracting decimals from non-decimal array."""
        array = pa.array([1, 2, 3], type=pa.int32())
        
        with pytest.raises(ValueError, match="Array type .* is not decimal"):
            self.handler.extract_decimal_from_arrow_array(array)
    
    def test_get_decimal_column_stats(self):
        """Test getting decimal column statistics."""
        # Create test table
        schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("amount", pa.decimal128(18, 4))
        ])
        
        decimals = [Decimal("100.1000"), Decimal("200.2000"), None, Decimal("300.3000")]
        data = [
            {"id": i, "amount": decimals[i] if i < len(decimals) else None}
            for i in range(4)
        ]
        
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df, schema=schema)
        
        stats = self.handler.get_decimal_column_stats(table, "amount")
        
        assert stats["column"] == "amount"
        assert stats["row_count"] == 4
        assert stats["null_count"] == 1
        assert stats["non_null_count"] == 3
        assert stats["min_value"] == "100.1000"
        assert stats["max_value"] == "300.3000"
        assert stats["precision"] == 18
        assert stats["scale"] == 4
    
    def test_get_decimal_column_stats_nonexistent_column(self):
        """Test getting stats for nonexistent column."""
        schema = pa.schema([pa.field("id", pa.int32())])
        table = pa.Table.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), schema=schema)
        
        with pytest.raises(ValueError, match="Column .* not found in table"):
            self.handler.get_decimal_column_stats(table, "nonexistent")
    
    def test_get_decimal_column_stats_non_decimal_column(self):
        """Test getting stats for non-decimal column."""
        schema = pa.schema([pa.field("id", pa.int32())])
        table = pa.Table.from_pandas(pd.DataFrame({"id": [1, 2, 3]}), schema=schema)
        
        with pytest.raises(ValueError, match="Column .* is not a decimal type"):
            self.handler.get_decimal_column_stats(table, "id")
    
    def test_compare_decimal_precision_end_to_end(self):
        """Test end-to-end decimal precision comparison."""
        source_values = [Decimal("123.45"), None, Decimal("678.90")]
        arrow_values = [Decimal("123.45"), None, Decimal("678.90")]
        target_values = [Decimal("123.45"), None, Decimal("678.90")]
        
        results = self.handler.compare_decimal_precision_end_to_end(
            source_values, arrow_values, target_values, 10, 2
        )
        
        assert results["total_compared"] == 3
        assert results["precision_matches"] == 3
        assert len(results["precision_mismatches"]) == 0
        assert results["null_handling_correct"] == 1
        assert results["precision_match_rate"] == 1.0
    
    def test_compare_decimal_precision_with_mismatches(self):
        """Test decimal precision comparison with mismatches."""
        source_values = [Decimal("123.45")]
        arrow_values = [Decimal("123.46")]  # Slight difference
        target_values = [Decimal("123.47")]  # Another difference
        
        results = self.handler.compare_decimal_precision_end_to_end(
            source_values, arrow_values, target_values, 10, 2
        )
        
        assert results["total_compared"] == 1
        assert results["precision_matches"] == 0
        assert len(results["precision_mismatches"]) == 1
        assert results["precision_match_rate"] == 0.0


if __name__ == "__main__":
    pytest.main([__file__])
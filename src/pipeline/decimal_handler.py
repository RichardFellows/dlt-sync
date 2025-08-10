"""High-precision decimal handling with Arrow decimal128 types."""

from typing import Dict, List, Any, Optional, Union, Tuple
from decimal import Decimal, getcontext, ROUND_HALF_UP
import pyarrow as pa
import pandas as pd
import numpy as np
import struct
import structlog

from ..utils.validation import validate_decimal_precision, ValidationError

logger = structlog.get_logger(__name__)

# Set decimal context for high precision
getcontext().prec = 38
getcontext().rounding = ROUND_HALF_UP


class DecimalHandler:
    """Handler for high-precision decimal operations with Arrow integration."""
    
    def __init__(self):
        """Initialize decimal handler."""
        self.type_mappings = self._build_type_mappings()
    
    def _build_type_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Build mapping from SQL Server types to Arrow types."""
        return {
            "decimal": {"arrow_type": pa.decimal128, "requires_precision": True},
            "numeric": {"arrow_type": pa.decimal128, "requires_precision": True},
            "money": {"arrow_type": pa.decimal128, "precision": 19, "scale": 4},
            "smallmoney": {"arrow_type": pa.decimal128, "precision": 10, "scale": 4},
            "float": {"arrow_type": pa.float64, "requires_precision": False},
            "real": {"arrow_type": pa.float32, "requires_precision": False},
            "bigint": {"arrow_type": pa.int64, "requires_precision": False},
            "int": {"arrow_type": pa.int32, "requires_precision": False},
            "smallint": {"arrow_type": pa.int16, "requires_precision": False},
            "tinyint": {"arrow_type": pa.uint8, "requires_precision": False},
            "bit": {"arrow_type": pa.bool_, "requires_precision": False}
        }
    
    def sql_type_to_arrow_type(
        self,
        sql_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> pa.DataType:
        """
        Convert SQL Server data type to Arrow data type.
        
        Args:
            sql_type: SQL Server data type name
            precision: Numeric precision (for decimal types)
            scale: Numeric scale (for decimal types)
            
        Returns:
            PyArrow data type
            
        Raises:
            ValueError: If type conversion is not supported
        """
        sql_type_lower = sql_type.lower()
        
        if sql_type_lower not in self.type_mappings:
            raise ValueError(f"Unsupported SQL Server type: {sql_type}")
        
        mapping = self.type_mappings[sql_type_lower]
        
        if mapping["requires_precision"]:
            if precision is None or scale is None:
                raise ValueError(f"Precision and scale required for {sql_type}")
            
            # Validate precision limits for Arrow decimal128
            if precision > 38:
                raise ValueError(f"Precision {precision} exceeds Arrow decimal128 limit of 38")
            
            return mapping["arrow_type"](precision, scale)
        
        elif "precision" in mapping and "scale" in mapping:
            # Fixed precision types like MONEY
            return mapping["arrow_type"](mapping["precision"], mapping["scale"])
        
        else:
            # Non-decimal types
            return mapping["arrow_type"]()
    
    def create_arrow_schema_from_sql_schema(
        self, 
        sql_schema: Dict[str, Dict[str, Any]]
    ) -> pa.Schema:
        """
        Create Arrow schema from SQL Server schema information.
        
        Args:
            sql_schema: Dictionary with column information from SQL Server
            
        Returns:
            PyArrow schema
        """
        fields = []
        
        for column_name, column_info in sql_schema.items():
            try:
                arrow_type = self.sql_type_to_arrow_type(
                    column_info["data_type"],
                    column_info.get("precision"),
                    column_info.get("scale")
                )
                
                nullable = column_info.get("is_nullable", True)
                
                field = pa.field(column_name, arrow_type, nullable=nullable)
                fields.append(field)
                
                logger.debug(
                    "Mapped SQL column to Arrow field",
                    column=column_name,
                    sql_type=column_info["data_type"],
                    arrow_type=str(arrow_type),
                    precision=column_info.get("precision"),
                    scale=column_info.get("scale")
                )
                
            except Exception as e:
                logger.error(
                    "Failed to map SQL column to Arrow field",
                    column=column_name,
                    sql_type=column_info.get("data_type"),
                    error=str(e)
                )
                raise
        
        return pa.schema(fields)
    
    def convert_pandas_decimals_to_arrow(
        self, 
        df: pd.DataFrame, 
        arrow_schema: pa.Schema
    ) -> pa.Table:
        """
        Convert pandas DataFrame with decimal columns to Arrow table.
        
        Args:
            df: Pandas DataFrame
            arrow_schema: Target Arrow schema
            
        Returns:
            PyArrow table with proper decimal types
        """
        try:
            # Convert decimal columns to string first to preserve precision
            converted_df = df.copy()
            
            for field in arrow_schema:
                if pa.types.is_decimal(field.type) and field.name in converted_df.columns:
                    # Convert to string to preserve precision during Arrow conversion
                    converted_df[field.name] = converted_df[field.name].astype(str)
            
            # Create Arrow table from pandas with schema
            table = pa.Table.from_pandas(converted_df, schema=arrow_schema, preserve_index=False)
            
            return table
            
        except Exception as e:
            logger.error("Failed to convert pandas decimals to Arrow", error=str(e))
            raise
    
    def validate_decimal_precision_in_table(
        self, 
        table: pa.Table,
        sql_schema: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Validate decimal precision in Arrow table against SQL schema.
        
        Args:
            table: PyArrow table
            sql_schema: SQL Server schema information
            
        Returns:
            List of validation issues found
        """
        issues = []
        
        for column_name in table.column_names:
            if column_name not in sql_schema:
                continue
            
            sql_column = sql_schema[column_name]
            sql_type = sql_column.get("data_type", "").lower()
            
            if sql_type not in ["decimal", "numeric", "money", "smallmoney"]:
                continue
            
            arrow_column = table.column(column_name)
            arrow_type = arrow_column.type
            
            if not pa.types.is_decimal(arrow_type):
                issues.append({
                    "column": column_name,
                    "issue": "Column should be decimal type in Arrow",
                    "sql_type": sql_type,
                    "arrow_type": str(arrow_type)
                })
                continue
            
            expected_precision = sql_column.get("precision")
            expected_scale = sql_column.get("scale")
            
            if sql_type == "money":
                expected_precision = 19
                expected_scale = 4
            elif sql_type == "smallmoney":
                expected_precision = 10
                expected_scale = 4
            
            if arrow_type.precision != expected_precision:
                issues.append({
                    "column": column_name,
                    "issue": "Precision mismatch",
                    "expected_precision": expected_precision,
                    "actual_precision": arrow_type.precision
                })
            
            if arrow_type.scale != expected_scale:
                issues.append({
                    "column": column_name,
                    "issue": "Scale mismatch",
                    "expected_scale": expected_scale,
                    "actual_scale": arrow_type.scale
                })
        
        return issues
    
    def format_decimal_for_sql_server(
        self, 
        value: Union[Decimal, float, str, None],
        precision: int,
        scale: int
    ) -> Optional[str]:
        """
        Format decimal value for SQL Server insertion.
        
        Args:
            value: Decimal value
            precision: Target precision
            scale: Target scale
            
        Returns:
            Formatted string for SQL Server, or None for NULL values
        """
        if value is None or pd.isna(value):
            return None
        
        try:
            if isinstance(value, str) and value.strip() == "":
                return None
            
            decimal_value = Decimal(str(value))
            
            # Validate precision and scale
            validate_decimal_precision(decimal_value, precision, scale)
            
            # Format with proper scale
            format_str = f"{{:.{scale}f}}"
            return format_str.format(decimal_value)
            
        except Exception as e:
            logger.error(
                "Failed to format decimal for SQL Server",
                value=str(value),
                precision=precision,
                scale=scale,
                error=str(e)
            )
            raise
    
    def extract_decimal_from_arrow_array(
        self, 
        array: pa.Array
    ) -> List[Optional[Decimal]]:
        """
        Extract decimal values from Arrow array as Python Decimal objects.
        
        Args:
            array: PyArrow decimal array
            
        Returns:
            List of Decimal values (None for nulls)
        """
        if not pa.types.is_decimal(array.type):
            raise ValueError(f"Array type {array.type} is not decimal")
        
        decimals = []
        scale = array.type.scale
        
        for i in range(len(array)):
            if array[i].is_valid:
                # Convert Arrow decimal128 to Python Decimal
                int_value = array[i].as_py()
                if isinstance(int_value, Decimal):
                    decimals.append(int_value)
                else:
                    # Convert from scaled integer
                    decimal_value = Decimal(int_value) / (Decimal(10) ** scale)
                    decimals.append(decimal_value)
            else:
                decimals.append(None)
        
        return decimals
    
    def compare_decimal_precision_end_to_end(
        self,
        source_values: List[Any],
        arrow_values: List[Any],
        target_values: List[Any],
        precision: int,
        scale: int
    ) -> Dict[str, Any]:
        """
        Compare decimal precision across the entire pipeline.
        
        Args:
            source_values: Values from SQL Server
            arrow_values: Values from Arrow processing
            target_values: Values from target system
            precision: Expected precision
            scale: Expected scale
            
        Returns:
            Comparison results
        """
        results = {
            "total_compared": len(source_values),
            "precision_matches": 0,
            "precision_mismatches": [],
            "null_handling_correct": 0,
            "conversion_errors": []
        }
        
        for i, (source, arrow, target) in enumerate(zip(source_values, arrow_values, target_values)):
            try:
                # Handle null values
                if source is None and arrow is None and target is None:
                    results["null_handling_correct"] += 1
                    results["precision_matches"] += 1
                    continue
                
                # Convert all to Decimal for comparison
                source_decimal = Decimal(str(source)) if source is not None else None
                arrow_decimal = Decimal(str(arrow)) if arrow is not None else None
                target_decimal = Decimal(str(target)) if target is not None else None
                
                # Compare values with tolerance for rounding
                tolerance = Decimal("0.1") ** scale
                
                if (source_decimal is not None and arrow_decimal is not None and 
                    target_decimal is not None):
                    
                    if (abs(source_decimal - arrow_decimal) <= tolerance and
                        abs(arrow_decimal - target_decimal) <= tolerance):
                        results["precision_matches"] += 1
                    else:
                        results["precision_mismatches"].append({
                            "index": i,
                            "source": str(source_decimal),
                            "arrow": str(arrow_decimal),
                            "target": str(target_decimal),
                            "source_to_arrow_diff": str(abs(source_decimal - arrow_decimal)),
                            "arrow_to_target_diff": str(abs(arrow_decimal - target_decimal))
                        })
                
            except Exception as e:
                results["conversion_errors"].append({
                    "index": i,
                    "source": str(source),
                    "arrow": str(arrow),
                    "target": str(target),
                    "error": str(e)
                })
        
        results["precision_match_rate"] = (
            results["precision_matches"] / results["total_compared"] 
            if results["total_compared"] > 0 else 0
        )
        
        return results
    
    def get_decimal_column_stats(
        self, 
        table: pa.Table, 
        column_name: str
    ) -> Dict[str, Any]:
        """
        Get statistics for a decimal column in Arrow table.
        
        Args:
            table: PyArrow table
            column_name: Column name
            
        Returns:
            Dictionary with column statistics
        """
        if column_name not in table.column_names:
            raise ValueError(f"Column {column_name} not found in table")
        
        column = table.column(column_name)
        
        if not pa.types.is_decimal(column.type):
            raise ValueError(f"Column {column_name} is not a decimal type")
        
        # Extract decimal values
        decimals = self.extract_decimal_from_arrow_array(column)
        non_null_decimals = [d for d in decimals if d is not None]
        
        if not non_null_decimals:
            return {
                "column": column_name,
                "row_count": len(decimals),
                "null_count": len(decimals),
                "non_null_count": 0,
                "min_value": None,
                "max_value": None,
                "precision": column.type.precision,
                "scale": column.type.scale
            }
        
        return {
            "column": column_name,
            "row_count": len(decimals),
            "null_count": len(decimals) - len(non_null_decimals),
            "non_null_count": len(non_null_decimals),
            "min_value": str(min(non_null_decimals)),
            "max_value": str(max(non_null_decimals)),
            "precision": column.type.precision,
            "scale": column.type.scale,
            "sample_values": [str(d) for d in non_null_decimals[:5]]
        }
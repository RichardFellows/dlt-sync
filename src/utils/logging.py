"""Structured logging utilities for the SQL Server incremental pipeline."""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import structlog
from datetime import datetime


def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[Path] = None,
    enable_structured: bool = True,
    enable_colors: bool = True
) -> structlog.BoundLogger:
    """
    Set up structured logging configuration.
    
    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        enable_structured: Enable structured JSON logging
        enable_colors: Enable colored console output
        
    Returns:
        Configured structlog BoundLogger
    """
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    
    # Configure structlog processors
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    
    if enable_structured:
        # Add timestamp and format as JSON for structured logging
        processors.extend([
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ])
    else:
        # Human-readable console output
        if enable_colors and sys.stdout.isatty():
            processors.append(structlog.dev.ConsoleRenderer(colors=True))
        else:
            processors.append(structlog.dev.ConsoleRenderer(colors=False))
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Set up file handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        
        if enable_structured:
            file_formatter = logging.Formatter('%(message)s')
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)
    
    return structlog.get_logger(name)


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a bound logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        BoundLogger instance
    """
    return structlog.get_logger(name)


class PipelineLogger:
    """Context-aware logger for pipeline operations."""
    
    def __init__(self, logger: structlog.BoundLogger, context: Optional[Dict[str, Any]] = None):
        """
        Initialize pipeline logger.
        
        Args:
            logger: Base structlog logger
            context: Initial context to bind to logger
        """
        self.base_logger = logger
        self.context = context or {}
        self._bound_logger = logger.bind(**self.context)
    
    def bind(self, **kwargs) -> "PipelineLogger":
        """
        Create new logger with additional context.
        
        Args:
            **kwargs: Additional context to bind
            
        Returns:
            New PipelineLogger with bound context
        """
        new_context = {**self.context, **kwargs}
        return PipelineLogger(self.base_logger, new_context)
    
    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self._bound_logger.info(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self._bound_logger.debug(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self._bound_logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self._bound_logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with context."""
        self._bound_logger.critical(message, **kwargs)
    
    def log_table_processing_start(self, table_type: str, table_name: str, **kwargs):
        """Log start of table processing."""
        self.bind(
            table_type=table_type,
            table_name=table_name,
            **kwargs
        ).info("Starting table processing")
    
    def log_table_processing_end(self, table_type: str, table_name: str, 
                               records_processed: int, duration_seconds: float, **kwargs):
        """Log end of table processing."""
        self.bind(
            table_type=table_type,
            table_name=table_name,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            records_per_second=records_processed / max(duration_seconds, 0.001),
            **kwargs
        ).info("Completed table processing")
    
    def log_batch_processing(self, batch_number: int, batch_size: int, 
                           total_batches: int, **kwargs):
        """Log batch processing progress."""
        self.bind(
            batch_number=batch_number,
            batch_size=batch_size,
            total_batches=total_batches,
            progress_percent=round((batch_number / total_batches) * 100, 1),
            **kwargs
        ).info("Processing batch")
    
    def log_partition_switch(self, source_table: str, target_table: str, 
                           partition_value: Any, success: bool, **kwargs):
        """Log partition switch operation."""
        level = "info" if success else "error"
        getattr(self.bind(
            source_table=source_table,
            target_table=target_table,
            partition_value=str(partition_value),
            success=success,
            **kwargs
        ), level)("Partition switch completed")
    
    def log_state_recovery(self, recovery_type: str, details: Dict[str, Any], **kwargs):
        """Log state recovery operation."""
        self.bind(
            recovery_type=recovery_type,
            recovery_details=details,
            **kwargs
        ).warning("State recovery performed")
    
    def log_decimal_precision_validation(self, table_name: str, column_name: str,
                                       source_precision: int, target_precision: int,
                                       validation_passed: bool, **kwargs):
        """Log decimal precision validation."""
        level = "info" if validation_passed else "error"
        getattr(self.bind(
            table_name=table_name,
            column_name=column_name,
            source_precision=source_precision,
            target_precision=target_precision,
            validation_passed=validation_passed,
            **kwargs
        ), level)("Decimal precision validation")
    
    def log_performance_metrics(self, operation: str, duration_seconds: float,
                               memory_usage_mb: Optional[float] = None,
                               records_processed: Optional[int] = None, **kwargs):
        """Log performance metrics."""
        metrics = {
            "operation": operation,
            "duration_seconds": duration_seconds,
        }
        
        if memory_usage_mb is not None:
            metrics["memory_usage_mb"] = memory_usage_mb
        
        if records_processed is not None:
            metrics["records_processed"] = records_processed
            metrics["records_per_second"] = records_processed / max(duration_seconds, 0.001)
        
        self.bind(**metrics, **kwargs).info("Performance metrics")


def create_pipeline_logger(name: str, config: Dict[str, Any]) -> PipelineLogger:
    """
    Create a pipeline logger from configuration.
    
    Args:
        name: Logger name
        config: Configuration dictionary containing log_level, log_file, etc.
        
    Returns:
        Configured PipelineLogger instance
    """
    base_logger = setup_logger(
        name=name,
        level=config.get("log_level", "INFO"),
        log_file=config.get("log_file"),
        enable_structured=config.get("enable_structured_logging", True),
        enable_colors=True
    )
    
    return PipelineLogger(base_logger)
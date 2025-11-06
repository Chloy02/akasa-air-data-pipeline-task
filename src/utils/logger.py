"""
Logging Configuration Module
Provides centralized logging setup for the application
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from src.config import LOGS_DIR, LOG_LEVEL


def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Setup and configure logger with file and console handlers
    
    Args:
        name: Logger name (typically module name)
        log_file: Optional specific log file name
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)
    
    # File Handler
    if log_file is None:
        log_file = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    file_handler = logging.FileHandler(LOGS_DIR / log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    return logger

"""
Logging configuration for the Fusion Report Engine.

Provides setup functions to configure console logging and defines placeholders
for production file-based rotating log handlers.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
from typing import Optional

# Default logger for the package
logger = logging.getLogger("fusion_report_engine")

def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None
) -> None:
    """
    Configure application-wide logging.

    Args:
        log_level: The logging severity level (default: logging.INFO).
        log_file: Optional file path to write logs to using a RotatingFileHandler.
    """
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Avoid duplicate handlers if setup is called multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # RotatingFileHandler placeholder:
    # To enable, pass a valid log_file path. In production, this can be managed
    # via settings or environment variables configured in config.py.
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Rotating file handler configuration placeholder
        rotating_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB limit
            backupCount=5,
            encoding="utf-8"
        )
        rotating_handler.setFormatter(formatter)
        rotating_handler.setLevel(log_level)
        root_logger.addHandler(rotating_handler)

    logger.info("Logging successfully initialized.")

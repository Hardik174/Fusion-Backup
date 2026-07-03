"""
Configuration settings for the Fusion Report Engine.

This module houses database credentials/connection placeholders, default options,
timezone settings, and other application-wide constants.

IMPORTANT:
- No SQL query strings.
- No hardcoded campaign IDs.
"""

from typing import Dict, Any

# Database Connection Placeholders
# These should be mapped or configured to read from Django settings / environment variables in production.
DATABASE_CONFIG: Dict[str, Any] = {
    "host": "",
    "port": 5432,
    "user": "",
    "password": "",
    "database": "",
}

# Default Pagination options
DEFAULT_PAGE_SIZE: int = 100

# Export parameters
EXCEL_SHEET_NAME: str = "Fusion Report"

# Timezone config
TIMEZONE: str = "Asia/Kolkata"

# Application-wide constants
APP_NAME: str = "Fusion Report Engine"
VERSION: str = "1.0.0"
SUPPORTED_EXPORT_FORMATS: list[str] = ["xlsx", "json"]

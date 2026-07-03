"""
Fusion Report Engine.

A clean, production-grade reporting engine designed as a Django backend service.
This package exposes the central ReportBuilder class and the helper ReportRequest dataclass.
"""

from .report_builder import ReportBuilder, ReportRequest

__all__ = ["ReportBuilder", "ReportRequest"]

"""Shared helpers for portable internal-lab and AWS result bundles."""

from .dashboard import metric_names_from_dashboard, patch_dashboard

__all__ = ["metric_names_from_dashboard", "patch_dashboard"]

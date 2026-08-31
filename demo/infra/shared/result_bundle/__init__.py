"""Shared helpers for portable internal-lab and AWS result bundles."""

from .dashboard import metric_names_from_dashboard, patch_dashboard
from .presentation import experiment_panel_markdown

__all__ = ["experiment_panel_markdown", "metric_names_from_dashboard", "patch_dashboard"]

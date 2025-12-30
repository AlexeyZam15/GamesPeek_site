"""
Middleware package for games app.
"""

from .database_optimization import DatabaseOptimizationMiddleware

__all__ = ['DatabaseOptimizationMiddleware']
# document-processor/app/tasks/celery.py
"""
Celery configuration
"""

from .processing import celery_app

__all__ = ('celery_app',)
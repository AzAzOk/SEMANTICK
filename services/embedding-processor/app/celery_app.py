from celery import Celery
from .core.config import settings

celery_app = Celery(
    "embedding_worker",
    broker=settings.RABBITMQ_URL,
    backend=settings.REDIS_URL,
    include=["app.tasks.embedding_task"]
)
celery_app.autodiscover_tasks([
    "app.tasks"
    ])
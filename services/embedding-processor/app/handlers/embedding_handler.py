import logging
from ..celery_app import celery_app
logger = logging.getLogger(__name__)


async def handle_embedding_event(payload: dict):
    """
    Обработка embedding.process события
    """
    if payload.get("type") != "embedding":
        logger.warning("Unknown event type")
        return

    task_id = payload.get("task_id")
    document = payload.get("document", {})
    chunks = document.get("chunks", [])

    if not task_id or not chunks:
        raise ValueError("Invalid embedding payload")

    logger.info(
        f"Embedding event received: task_id={task_id}, "
        f"chunks={len(chunks)}"
    )

    celery_app.send_task(
        name="embedding.create",
        kwargs={
            "task_id": task_id,
            "document": document
        }
    )

    # Пока: заглушка
    logger.info("Embedding data prepared (not processed yet)")

from ..celery_app import celery_app
from ..core.embedder import Embedder
from ..core.qdrant_client import client
from qdrant_client.http.models import PointStruct
import logging
from ..core.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(name="embedding.create")
def create_embeddings(task_id: str, document: dict) -> dict:
    """
    Создание embedding-ов и сразу upsert в Qdrant.
    Возвращает только статус и количество точек.
    """
    embedder = Embedder()
    chunks = document.get("chunks", [])

    if not chunks:
        logger.info(f"No chunks to process for task_id={task_id}")
        return {"task_id": task_id, "status": "no_chunks", "count": 0}

    # Получаем embeddings
    texts = [chunk["text"] for chunk in chunks]
    vectors = embedder.embed(texts)

    # Формируем точки для Qdrant
    points = []
    for chunk, vector in zip(chunks, vectors):
        point = PointStruct(
            id =  chunk["chunk_id"],  # уникальный идентификатор
            vector = vector,
            payload = {
                "text": chunk["text"],
                "metadata": chunk["metadata"],
                "business_metadata": chunk.get("business_metadata"),
                "file_name": document.get("file_name"),
                "file_extension": document.get("file_extension"),
            }
        )
        points.append(point)

    # Upsert в Qdrant
    try:
        client.upsert(collection_name=settings.COLLECTION_NAME, points=points)
    except Exception as e:
        logger.error(f"Qdrant upsert failed for task_id={task_id}: {e}")
        return {"task_id": task_id, "status": "failed", "count": 0}

    logger.info(
        f"Embeddings upserted to Qdrant: task_id={task_id}, points={len(points)}"
    )

    # 4️⃣ Возвращаем только status + count
    return {"task_id": task_id, "status": "success", "count": len(points)}

from qdrant_client import QdrantClient
from qdrant_client.http import models
from typing import List, Dict, Any
import requests
import logging
logger = logging.getLogger(__name__)
from app.config import (
    QDRANT_HOST,
    QDRANT_PORT,
    COLLECTION_NAME,
    OLLAMA_MODEL,
    OLLAMA_URL
)

# ===============================
# CLIENT
# ===============================

client = QdrantClient(
    host=QDRANT_HOST,
    port=QDRANT_PORT,
)

# ===============================
# INIT-ФУНКЦИИ
# ===============================


def init_qdrant() -> None:
    """
    Проверка доступности Qdrant.
    Вызывается ЯВНО (FastAPI startup / Celery init).
    """
    try:
        client.get_collections()
        logger.info("Qdrant доступен")
    except Exception as e:
        logger.info(f"Qdrant недоступен: {e}")
        raise


def create_document_collection() -> None:
    """
    Создаёт коллекцию, если она не существует.
    """
    if client.collection_exists(COLLECTION_NAME):
        logger.info(f"ℹКоллекция '{COLLECTION_NAME}' уже существует")
        return

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=1024,
            distance=models.Distance.COSINE
        )
    )

    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="customer",
        field_schema=models.PayloadSchemaType.KEYWORD
    )

    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="code",
        field_schema=models.PayloadSchemaType.KEYWORD
    )

    logger.info(f"Коллекция '{COLLECTION_NAME}' создана")


# ===============================
# OLLAMA
# ===============================

def get_embedding(text: str, model: str = OLLAMA_MODEL) -> List[float]:
    """
    Получает embedding из Ollama.
    """
    response = requests.post(
        f"{OLLAMA_URL}/api/embeddings",
        json={
            "model": model,
            "prompt": text
        },
        timeout=60
    )

    if response.status_code != 200:
        raise RuntimeError(f"Ollama error: {response.text}")

    return response.json()["embedding"]


# ===============================
# QDRANT OPS
# ===============================

def create_embeddings_from_chunks(
    chunks: List[Dict[str, Any]],
    model: str = OLLAMA_MODEL
) -> List[models.PointStruct]:
    """
    Создает PointStruct'ы для Qdrant.
    """
    points: List[models.PointStruct] = []

    for chunk in chunks:
        embedding = get_embedding(chunk["text"], model)

        point = models.PointStruct(
            id=chunk["chunk_id"],
            vector=embedding,
            payload={
                "text": chunk["text"],
                "word_count": chunk.get("word_count"),
                "char_count": chunk.get("char_count"),
                "metadata": chunk.get("metadata"),
                "business_metadata": chunk.get("business_metadata"),
            }
        )

        points.append(point)

    return points


def add_chunks_to_qdrant(
    chunks: List[Dict[str, Any]],
    model: str = OLLAMA_MODEL
) -> int:
    """
    Добавляет чанки в Qdrant.
    Возвращает количество добавленных точек.
    """
    if not chunks:
        logger.info("Нет чанков для загрузки в Qdrant")
        return 0

    points = create_embeddings_from_chunks(chunks, model)

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )

    logger.info(f"Загружено {len(points)} чанков в Qdrant")
    return len(points)


def reserch_similar_chunks(
    query: str,
    top_k: int = 5,
    model: str = OLLAMA_MODEL
) -> List[Dict[str, Any]]:
    """
    Поиск похожих чанков.
    """
    query_embedding = get_embedding(query, model)

    search_result = client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_embedding,
        limit=top_k,
        with_payload=True
    )

    return [
        {
            "id": point.id,
            "score": point.score,
            "text": point.payload.get("text"),
            "word_count": point.payload.get("word_count"),
            "char_count": point.payload.get("char_count"),
            "metadata": point.payload.get("metadata"),
            "business_metadata": point.payload.get("business_metadata"),
        }
        for point in search_result
    ]


def reserch_file_name(
    query_file_name: str,
) -> bool:
    """
    Поиск идентичных чанков по названию.
    """
    try:
        normalized_name = query_file_name.lower()
        search_result = client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=models.Filter(
                must=[models.FieldCondition(
                    key="metadata.file_name",
                    match=models.MatchValue(value=normalized_name)
                    )
                ]
            ),
            limit=1,
            with_payload=True,
            with_vectors=False
        )

        points, _ = search_result

        return len(points) > 0
    except Exception as e:
        logger.info(f"Ошибка при поиске чанков: {e}")
        return False

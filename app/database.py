from qdrant_client import QdrantClient
from qdrant_client.http import models

client = QdrantClient(host="localhost", port=6333)

try:
    collections = client.get_collections()
    print("Успешное подключение к Qdrant!")
    print(f"Доступные коллекции: {[col.name for col in collections.collections]}")
except Exception as e:
    print(f" Ошибка подключения: {e}")

def create_document_collection():
    """Создает коллекцию для документов"""
    
    collection_name = "document_archive"
    
    try:
        client.get_collection(collection_name)
        print(f"Коллекция {collection_name} уже существует")
        return
    except Exception:
        print(f"Создаем коллекцию {collection_name}...")
    
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=1024,
            distance=models.Distance.COSINE
        )
    )
    
    client.create_payload_index(
        collection_name=collection_name,
        field_name="customer",
        field_schema=models.PayloadSchemaType.KEYWORD
    )
    
    client.create_payload_index(
        collection_name=collection_name, 
        field_name="code",
        field_schema=models.PayloadSchemaType.KEYWORD
    )
    
    print(f"Коллекция {collection_name} создана!")

create_document_collection()

import requests
from typing import List, Dict, Any

def create_embeddings_from_chunks(chunks: List[Dict[str, Any]], model: str = "bge-m3:567m") -> List[Dict[str, Any]]:
    """
    Создает эмбеддинги для списка чанков через Ollama и возвращает список
    объектов, готовых для вставки в Qdrant.
    """
    points: List[Dict[str, Any]] = []

    for chunk in chunks:
        text = chunk["text"]
        chunk_id = chunk["chunk_id"]

        # Запрос в Ollama
        response = requests.post(
            "http://localhost:11434/api/embeddings",
            json={"model": model, "prompt": text}
        )

        if response.status_code != 200:
            raise RuntimeError(f"Ollama error for chunk {chunk_id}: {response.text}")

        embedding = response.json()["embedding"]

        # Готовим point для Qdrant
        points.append({
            "id": chunk_id,
            "vector": embedding,
            "payload": {
                "text": text,
                "word_count": chunk.get("word_count"),
                "char_count": chunk.get("char_count"),
                "metadata": chunk.get("metadata"),
                "business_metadata": chunk.get("business_metadata"),
            }
        })

    return points


from qdrant_client.http import models

def add_chunks_to_qdrant(client, chunks: List[Dict[str, Any]], model: str = "bge-m3:567m") -> None:
    """
    Создает эмбеддинги и добавляет все чанки в Qdrant одной операцией.
    """
    points = create_embeddings_from_chunks(chunks, model)

    if not points:
        # Нечего заливать — логируем и возвращаем пустой список
        print("⚠️ create_embeddings_from_chunks вернул 0 точек — пропускаем upsert в Qdrant")
        return []

    client.upsert(
        collection_name="document_archive",
        points=[models.PointStruct(**p) for p in points]
    )

    print(f"✅ Загружено {len(points)} чанков в Qdrant")
    return points

def reserch_similar_chunks(client, query: str, top_k: int = 15, model: str = "bge-m3:567m") -> List[Dict[str, Any]]:
    """
    Выполняет поиск похожих чанков в Qdrant по текстовому запросу.
    """
    # Получаем эмбеддинг для запроса
    response = requests.post(
        "http://localhost:11434/api/embeddings",
        json={"model": model, "prompt": query}
    )

    if response.status_code != 200:
        raise RuntimeError(f"Ollama error for query: {response.text}")

    query_embedding = response.json()["embedding"]

    # Выполняем поиск в Qdrant
    search_result = client.search(
        collection_name="document_archive",
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
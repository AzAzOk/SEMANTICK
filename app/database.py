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
    
    # Создаем коллекцию
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=1024,  # Для multilingual-e5-large
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

# create_document_collection()
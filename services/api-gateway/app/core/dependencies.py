"""
Dependency Injection для API Gateway
Предоставляет shared ресурсы (HTTP клиенты)
"""

import httpx
from .config import settings
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# HTTP CLIENTS FOR MICROSERVICES
# ============================================================================

class SearchServiceClient:
    """
    HTTP клиент для взаимодействия с Search Service
    
    TODO: Реализовать в следующей итерации миграции
    """
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
    
    async def search(self, query: str, top_k: int = 5):
        """Выполнить семантический поиск"""
        response = await self.client.post(
            "/search",
            json={"query": query, "top_k": top_k}
        )
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Закрыть HTTP клиент"""
        await self.client.aclose()


class DocumentProcessorClient:
    """
    HTTP клиент для взаимодействия с Document Processor Service
    
    TODO: Реализовать в следующей итерации миграции
    """
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=120.0)
    
    async def process_file(self, file_path: str):
        """Отправить файл на обработку"""
        response = await self.client.post(
            "/process",
            json={"file_path": file_path}
        )
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Закрыть HTTP клиент"""
        await self.client.aclose()


def get_search_service_client() -> SearchServiceClient:
    """
    Получить HTTP клиент для Search Service
    
    Returns:
        Экземпляр SearchServiceClient
    """
    return SearchServiceClient(settings.SEARCH_SERVICE_URL)


def get_document_processor_client() -> DocumentProcessorClient:
    """
    Получить HTTP клиент для Document Processor Service
    
    Returns:
        Экземпляр DocumentProcessorClient
    """
    return DocumentProcessorClient(settings.DOCUMENT_PROCESSOR_URL)

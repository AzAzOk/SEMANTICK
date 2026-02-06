# services/api-gateway/app/main.py
"""
API Gateway - Main Entry Point (без Celery)
"""

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
import logging

# Импорты модулей
from .api.routes import create_routes, create_lifespan, add_middleware
from .api.websocket import websocket_endpoint
from .core.config import settings
from .core.dependencies import get_search_service_client, get_document_processor_client

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_application() -> FastAPI:
    """Создание и настройка FastAPI приложения"""
    
    # Получение HTTP клиентов для микросервисов
    search_service_client = get_search_service_client()
    document_processor_client = get_document_processor_client()
    
    # Создание lifespan (startup/shutdown)
    lifespan = create_lifespan(search_service_client)
    
    # Инициализация FastAPI
    app = FastAPI(
        title="Semantic Search API Gateway",
        description="API Gateway для системы семантического поиска (микросервисная архитектура)",
        version="2.0.0",
        lifespan=lifespan
    )
    
    # Подключение статических файлов
    try:
        app.mount("/static", StaticFiles(directory="app/static"), name="static")
    except Exception as e:
        logger.warning(f"Static files directory not found: {e}")
    
    # Регистрация HTTP routes
    create_routes(
        app=app,
        search_service_client=search_service_client,
        document_processor_client=document_processor_client
    )
    
    # Регистрация WebSocket endpoint
    @app.websocket("/ws/{client_id}")
    async def ws_handler(websocket: WebSocket, client_id: str):
        """WebSocket для real-time мониторинга задач"""
        await websocket_endpoint(websocket, client_id)
    
    # Добавление middleware
    add_middleware(app)
    
    logger.info("✅ API Gateway application created successfully")
    return app


# Создание приложения
app = create_application()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info"
    )
    
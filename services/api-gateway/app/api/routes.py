# services/api-gateway/app/api/routes.py
"""
HTTP Routes для API Gateway (без Celery)
"""

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from pathlib import Path
from contextlib import asynccontextmanager
import logging
import uuid
import asyncio
from ..core.config import settings
from ..core.rabbitmq_publisher import publisher
from ..core.redis_client import task_status_manager

logger = logging.getLogger(__name__)

# ==========================================
# MODELS
# ==========================================

class SearchRequest(BaseModel):
    text: str


# ==========================================
# LIFESPAN MANAGEMENT
# ==========================================

def create_lifespan(search_service_client):
    """Создание lifespan context manager для FastAPI"""
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ===== STARTUP =====
        logger.info("🚀 API Gateway startup...")
        
        # Подключение к RabbitMQ
        try:
            await publisher.connect()
            logger.info("✅ RabbitMQ publisher ready")
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            raise
        
        # Подключение к Redis
        try:
            await task_status_manager.connect()
            logger.info("✅ Redis task manager ready")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
        
        logger.info("✅ API Gateway startup completed")
        
        yield
        
        # ===== SHUTDOWN =====
        logger.info("🛑 API Gateway shutdown...")
        
        # Закрытие RabbitMQ
        try:
            await publisher.close()
        except Exception as e:
            logger.error(f"Error closing RabbitMQ: {e}")
        
        # Закрытие Redis
        try:
            await task_status_manager.close()
        except Exception as e:
            logger.error(f"Error closing Redis: {e}")
        
        # Закрытие HTTP клиентов
        try:
            await search_service_client.close()
        except Exception as e:
            logger.error(f"Error closing search service client: {e}")
        
        logger.info("🛑 API Gateway shutdown completed")
    
    return lifespan


# ==========================================
# MIDDLEWARE
# ==========================================

def add_middleware(app: FastAPI):
    """Добавление middleware в приложение"""
    
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Connection optimization
    @app.middleware("http")
    async def optimize_connection_headers(request, call_next):
        response = await call_next(request)
        
        if request.url.path.startswith("/ws") or request.url.path.startswith("/task-status"):
            response.headers["Connection"] = "keep-alive"
            response.headers["Keep-Alive"] = "timeout=60, max=1000"
        else:
            response.headers["Connection"] = "close"
        
        return response
    
    logger.info("✅ Middleware configured")


# ==========================================
# ROUTES REGISTRATION
# ==========================================

def create_routes(app: FastAPI, search_service_client, document_processor_client):
    """Регистрация всех HTTP routes"""
    
    # ==========================================
    # STATIC PAGES
    # ==========================================
    
    @app.get("/")
    async def root():
        return {
            "service": "API Gateway",
            "version": "2.0.0",
            "status": "running",
            "endpoints": {
                "pages": ["/index", "/semantic"],
                "docs": ["/docs", "/redoc"],
                "api": ["/search", "/upload", "/tasks"],
                "websocket": "/ws/{client_id}"
            }
        }

    @app.get("/semantic", response_class=HTMLResponse)
    async def semantic_page():
        path = Path("app/templates/semantic.html")
        if not path.exists():
            raise HTTPException(status_code=404, detail="Template not found")
        return path.read_text(encoding="utf-8")

    @app.get("/index", response_class=HTMLResponse)
    async def index_page():
        path = Path("app/templates/index.html")
        if not path.exists():
            raise HTTPException(status_code=404, detail="Template not found")
        return path.read_text(encoding="utf-8")

    # ==========================================
    # FILE UPLOAD (через RabbitMQ)
    # ==========================================

    @app.post("/select-file")
    async def select_file(request: Request, file: List[UploadFile] = File(...)):
        """
        Загрузка отдельных файлов
        Создает задачи в Redis и публикует в RabbitMQ
        """
        uploaded_files = []
        task_ids = []
        
        try:
            # Создаем директорию для загрузок
            uploads_dir = Path(settings.UPLOAD_DIR)
            uploads_dir.mkdir(parents=True, exist_ok=True)
            
            for uploaded_file in file:
                # Генерируем уникальный task_id
                task_id = str(uuid.uuid4())
                
                # Сохраняем файл
                file_path = uploads_dir / uploaded_file.filename
                contents = await uploaded_file.read()
                
                with open(file_path, "wb") as f:
                    f.write(contents)
                
                # Создаем запись в Redis
                await task_status_manager.create_task(
                    task_id=task_id,
                    filename=uploaded_file.filename
                )
                
                # Публикуем задачу в RabbitMQ
                success = await publisher.publish_file_task(
                    task_id=task_id,
                    file_path=str(file_path),
                    filename=uploaded_file.filename
                )
                
                if not success:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to publish task for {uploaded_file.filename}"
                    )
                
                uploaded_files.append({
                    "filename": uploaded_file.filename,
                    "size": len(contents),
                    "task_id": task_id
                })
                task_ids.append(task_id)
                
                logger.info(f"📤 File uploaded: {uploaded_file.filename} -> task {task_id}")
            
            return {
                "status": "accepted",
                "message": f"Accepted {len(file)} file(s) for processing",
                "files": uploaded_files,
                "task_ids": task_ids
            }
            
        except Exception as e:
            logger.error(f"❌ File upload error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/select-folder")
    async def select_folder(
        request: Request, 
        file: List[UploadFile] = File(...), 
        folder_name: Optional[str] = None
    ):
        """
        Загрузка папки с файлами
        Создает одну задачу для всей папки
        """
        try:
            uploads_dir = Path("uploads")
            uploads_dir.mkdir(exist_ok=True)
            
            # Определяем имя папки
            if not folder_name and file:
                folder_name = file[0].filename.split("/")[0] if "/" in file[0].filename else "uploaded_folder"
            
            # Генерируем task_id для всей папки
            task_id = str(uuid.uuid4())
            
            # Сохраняем все файлы
            file_paths = []
            for uploaded_file in file:
                file_path = uploads_dir / uploaded_file.filename
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                contents = await uploaded_file.read()
                with open(file_path, "wb") as f:
                    f.write(contents)
                
                file_paths.append(str(file_path))
            
            # Создаем запись в Redis
            await task_status_manager.create_task(
                task_id=task_id,
                filename=f"Folder: {folder_name} ({len(file)} files)"
            )
            
            # Публикуем задачу в RabbitMQ
            success = await publisher.publish_folder_task(
                task_id=task_id,
                file_paths=file_paths,
                folder_name=folder_name
            )
            
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to publish folder task"
                )
            
            logger.info(f"📤 Folder uploaded: {folder_name} ({len(file)} files) -> task {task_id}")
            
            return {
                "status": "accepted",
                "message": f"Accepted folder '{folder_name}' with {len(file)} files",
                "folder_name": folder_name,
                "task_id": task_id,
                "files_count": len(file)
            }
            
        except Exception as e:
            logger.error(f"❌ Folder upload error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ==========================================
    # SEMANTIC SEARCH
    # ==========================================

    @app.post("/message")
    async def message_input(search_request: SearchRequest):
        """Поиск по семантическому запросу"""
        if not search_request.text or not search_request.text.strip():
            return {
                "status": "error",
                "message": "Текст запроса не может быть пустым",
                "results": []
            }

        try:
            # Вызов Search Service через HTTP
            search_result = await search_service_client.search(search_request.text)
            
            if not search_result or not search_result.get('results'):
                return {
                    "status": "no_results",
                    "message": "По вашему запросу ничего не найдено",
                    "results": []
                }

            results = search_result.get('results', [])
            top_results = results[:5]
            
            formatted_results = [
                {
                    "rank": i,
                    "id": r.get('id', f'result_{i}'),
                    "score": r.get('score', 0) * 100,
                    "text": r.get('text', ''),
                    "file_name": r.get('metadata', {}).get('file_name', 'Неизвестно'),
                    "file_path": r.get('metadata', {}).get('file_path', 'Неизвестно'),
                    "file_extension": r.get('metadata', {}).get('file_extension', 'Неизвестно'),
                    "chunk_index": r.get('metadata', {}).get('chunk_index', 0)
                } for i, r in enumerate(top_results, 1)
            ]
            
            return {
                "status": "success",
                "message": f"Найдено результатов: {len(results)}",
                "count": len(formatted_results),
                "results": formatted_results
            }
            
        except Exception as e:
            logger.exception("❌ Search error")
            return {
                "status": "error",
                "message": f"Ошибка при выполнении поиска: {str(e)}",
                "results": []
            }

    # ==========================================
    # TASK STATUS (читаем из Redis)
    # ==========================================

    @app.get("/task-status/{task_id}")
    async def get_task_status(task_id: str):
        """Получение статуса задачи из Redis"""
        try:
            status_data = await task_status_manager.get_task_status(task_id)
            
            if not status_data:
                return {
                    "task_id": task_id,
                    "status": "not_found",
                    "message": "Task not found or expired"
                }
            
            return status_data
            
        except Exception as e:
            logger.error(f"❌ Error getting task status: {e}")
            return {
                "task_id": task_id,
                "status": "error",
                "error": str(e)
            }
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "service": "api-gateway",
            "version": "2.0.0"
        }
    
    logger.info("✅ All routes registered")
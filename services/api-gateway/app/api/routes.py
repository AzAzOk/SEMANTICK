# services/api-gateway/app/api/routes.py
"""
HTTP Routes для API Gateway
Обрабатывает загрузку файлов, семантический поиск и управление задачами
"""

from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from pathlib import Path
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

# ==========================================
# MODELS
# ==========================================

class SearchRequest(BaseModel):
    text: str


# ==========================================
# HELPER FUNCTIONS
# ==========================================

# async def cleanup_tasks_and_files(task_ids: List[str], file_paths: List[str], celery_app):
#     """Отмена задач Celery и удаление файлов"""
#     for t in task_ids:
#         try:
#             celery_app.control.revoke(t, terminate=True, signal='SIGKILL')
#         except Exception as e:
#             logger.warning(f"Ошибка отмены задачи {t}: {str(e)}")
#     for f in file_paths:
#         try:
#             Path(f).unlink()
#             logger.info(f"Удалён файл: {f}")
#         except FileNotFoundError:
#             pass
#         except Exception as e:
#             logger.warning(f"Ошибка удаления файла {f}: {str(e)}")


async def normalize_error(raw) -> dict:
    """Нормализация ошибок для единообразного формата"""
    if isinstance(raw, dict) and 'exc_type' in raw:
        return {'type': raw['exc_type'],
                'message': str(raw.get('exc_message') or raw.get('exc_args', ''))}
    if isinstance(raw, dict) and 'type' in raw:
        return raw
    if isinstance(raw, BaseException):
        return {'type': type(raw).__name__, 'message': str(raw)}
    return {'type': 'Exception',
            'message': str(raw) if raw else 'Unknown error'}


# ==========================================
# LIFESPAN MANAGEMENT
# ==========================================

def create_lifespan(search_service_client):
    """
    Создание lifespan context manager для FastAPI
    
    Args:
        search_service_client: HTTP клиент для Search Service
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        logger.info("API Gateway startup...")
        
        # TODO: Инициализация соединений с микросервисами
        # - Проверка доступности Search Service
        # - Проверка доступности Embedding Service
        # - Проверка доступности Document Processor
        
        logger.info("✅ API Gateway startup completed")
        
        yield
        
        # Shutdown
        logger.info("🛑 API Gateway shutdown")
        
        # Закрытие HTTP клиентов
        try:
            await search_service_client.close()
            logger.info("Search service client closed")
        except Exception as e:
            logger.error(f"Error closing search service client: {e}")
    
    return lifespan


# ==========================================
# MIDDLEWARE
# ==========================================

def add_middleware(app: FastAPI):
    """Добавление middleware в приложение"""
    
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # В production указать конкретные origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Middleware для оптимизации соединений
    @app.middleware("http")
    async def optimize_connection_headers(request, call_next):
        """Оптимизированные заголовки для HTTP соединений"""
        response = await call_next(request)
        
        if request.url.path.startswith("/ws") or request.url.path.startswith("/task-status"):
            response.headers["Connection"] = "keep-alive"
            response.headers["Keep-Alive"] = "timeout=60, max=1000"
        else:
            response.headers["Connection"] = "close"
        
        return response
    
    logger.info("Middleware added")


# ==========================================
# ROUTES REGISTRATION
# ==========================================

def create_routes(app: FastAPI, search_service_client, document_processor_client): # def create_routes(app: FastAPI, celery_app, search_service_client, document_processor_client):
    """
    Регистрация всех HTTP routes в FastAPI приложении
    
    Args:
        app: FastAPI приложение
        celery_app: Celery приложение для управления задачами
        search_service_client: Клиент для обращения к Search Service
        document_processor_client: Клиент для обращения к Document Processor
    """
    
    # Импортируем WebSocket manager для отправки обновлений
    from .websocket import manager as ws_manager
    
    # ==========================================
    # STATIC PAGES
    # ==========================================
    
    @app.get("/")
    async def root():
        return {
            "message": "Semantic Search API",
            "endpoints": [
                "/index - Main page",
                "/docs - Swagger UI",
                "/redoc - ReDoc",
                "/tasks/active - Active tasks",
                "/semantic - Semantic UI",
                "/ws/{client_id} - WebSocket connection"
            ]
        }

    @app.get("/semantic", response_class=HTMLResponse)
    async def semantic_page():
        path = Path("app/templates/semantic.html")
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        return path.read_text(encoding="utf-8")

    @app.get("/index", response_class=HTMLResponse)
    async def index_page():
        path = Path("app/templates/index.html")
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        return path.read_text(encoding="utf-8")

    # ==========================================
    # TASK MANAGEMENT
    # ==========================================

    # @app.delete("/task-cancel/{task_id}")
    # async def cancel_task(task_id: str):
    #     """Отмена задачи Celery по ID"""
    #     try:
    #         celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
    #         logger.info(f"Task {task_id} cancelled")
            
    #         await ws_manager.send_task_update(task_id, {
    #             "task_id": task_id,
    #             "type": "task_update",
    #             "status": "cancelled",
    #             "message": "Задача отменена пользователем"
    #         })
            
    #         return {"status": "cancelled", "task_id": task_id, "message": "Задача отменена"}
    #     except Exception as e:
    #         logger.exception(f"Ошибка отмены задачи {task_id}")
    #         raise HTTPException(status_code=500, detail=f"Ошибка отмены: {str(e)}")

    # @app.post("/tasks-cancel-batch")
    # async def cancel_tasks_batch(task_ids: List[str]):
    #     """Пакетная отмена задач"""
    #     cancelled, errors = [], []
    #     for task_id in task_ids:
    #         try:
    #             celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
    #             cancelled.append(task_id)
    #             logger.info(f"Задача {task_id} отменена")
                
    #             await ws_manager.send_task_update(task_id, {
    #                 "task_id": task_id,
    #                 "type": "task_update",
    #                 "status": "cancelled",
    #                 "message": "Задача отменена пользователем"
    #             })
    #         except Exception as e:
    #             errors.append({"task_id": task_id, "error": str(e)})
    #             logger.warning(f"Ошибка отмены {task_id}: {str(e)}")
    #     return {
    #         "status": "completed",
    #         "cancelled": cancelled,
    #         "cancelled_count": len(cancelled),
    #         "errors": errors,
    #         "errors_count": len(errors)
    #     }

    # @app.get("/tasks/active")
    # async def get_active_tasks():
    #     """Получение списка активных задач"""
    #     inspect = celery_app.control.inspect()
    #     active_tasks = inspect.active() or {}
    #     tasks_list = [
    #         {"task_id": t.get('id'), "name": t.get('name'), "worker": worker}
    #         for worker, tasks in active_tasks.items() if tasks
    #         for t in tasks
    #     ]
    #     return {"active_tasks": tasks_list, "count": len(tasks_list)}

    # ==========================================
    # FILE UPLOAD
    # ==========================================

    @app.post("/select-file")
    async def select_file(request: Request, file: List[UploadFile] = File(...)):
        """
        Загрузка отдельных файлов
        
        TODO: В микросервисной архитектуре здесь будет проксирование
        к Document Processor Service через HTTP/gRPC
        """
        
        

        # Временная заглушка - вернуть ошибку с информацией
        raise HTTPException(
            status_code=501,
            detail="File upload endpoint not yet migrated to microservices. "
                   "Please implement Document Processor service integration."
        )

    @app.post("/select-folder")
    async def select_folder(
        request: Request, 
        file: List[UploadFile] = File(...), 
        folder_name: Optional[str] = None
    ):
        """
        Загрузка папки с файлами
        
        TODO: В микросервисной архитектуре здесь будет проксирование
        к Document Processor Service через HTTP/gRPC
        """
        # Временная заглушка
        raise HTTPException(
            status_code=501,
            detail="Folder upload endpoint not yet migrated to microservices. "
                   "Please implement Document Processor service integration."
        )

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
            # Вызов к Search Service через HTTP клиент
            search_result = await search_service_client.search(search_request.text)
            
            if not search_result or not search_result.get('results'):
                return {
                    "status": "no_results", 
                    "message": "По вашему запросу ничего не найдено", 
                    "results": []
                }

            # Форматирование результатов
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
            logger.exception("Ошибка поиска")
            return {
                "status": "error", 
                "message": f"Ошибка при выполнении поиска: {str(e)}", 
                "results": []
            }

    # ==========================================
    # LEGACY TASK STATUS (HTTP polling fallback)
    # ==========================================

    # @app.get("/task-status/{task_id}")
    # async def get_task_status(task_id: str):
    #     """Проверка статуса задачи по ID - legacy endpoint для обратной совместимости"""
    #     try:
    #         task = celery_app.AsyncResult(task_id)
    #         state = task.state

    #         if state == 'PENDING':
    #             return {
    #                 "task_id": task_id, 
    #                 "status": "pending", 
    #                 "progress": 0, 
    #                 "current_step": 1, 
    #                 "total_steps": 6, 
    #                 "message": "Задача в очереди..."
    #             }
    #         elif state == 'PROGRESS':
    #             info = task.info or {}
    #             return {
    #                 "task_id": task_id,
    #                 "status": "processing",
    #                 "progress": info.get('progress', 0),
    #                 "current_step": info.get('current_step', 0),
    #                 "total_steps": info.get('total_steps', 6),
    #                 "message": info.get('status', 'Обработка...'),
    #                 "filename": info.get('filename', '')
    #             }
    #         elif state == 'SUCCESS':
    #             result_data = task.result or {}
    #             return {
    #                 "task_id": task_id,
    #                 "status": "completed",
    #                 "progress": 100,
    #                 "current_step": 6,
    #                 "total_steps": 6,
    #                 "result": result_data,
    #                 "message": "Обработка завершена"
    #             }
    #         elif state == 'FAILURE':
    #             error_info = await normalize_error(task.info or {})
    #             return {"task_id": task_id, "state": "FAILURE", "error": error_info}
    #         else:
    #             return {
    #                 "task_id": task_id, 
    #                 "status": state.lower(), 
    #                 "message": str(task.info)
    #             }

    #     except Exception as e:
    #         logger.exception("Ошибка получения статуса задачи")
    #         return {
    #             "task_id": task_id, 
    #             "state": "ERROR", 
    #             "error": f"Unexpected error: {str(e)}"
    #         }
    
    logger.info("All routes registered")
from fastapi import FastAPI, UploadFile, File, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from contextlib import asynccontextmanager
from pathlib import Path
import json
from app.database import (
    init_qdrant,
    create_document_collection,
    reserch_similar_chunks
)
from app.tasks.tasks_parsing import (
    generate_embedding,
    generate_embedding_batch,
    celery_app
)
from typing import Optional, List, Dict
import logging
import shutil
import asyncio
import json

logger = logging.getLogger(__name__)

# ----------------- Модели -----------------
class SearchRequest(BaseModel):
    text: str

# ----------------- WebSocket Manager -----------------
class ConnectionManager:
    """Менеджер WebSocket соединений для отслеживания статуса задач"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.task_subscriptions: Dict[str, set] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"WebSocket клиент подключен: {client_id}")
    
    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        for task_id in list(self.task_subscriptions.keys()):
            if client_id in self.task_subscriptions[task_id]:
                self.task_subscriptions[task_id].discard(client_id)
                if not self.task_subscriptions[task_id]:
                    del self.task_subscriptions[task_id]
        logger.info(f"WebSocket клиент отключен: {client_id}")
    
    def subscribe_to_task(self, client_id: str, task_id: str):
        if task_id not in self.task_subscriptions:
            self.task_subscriptions[task_id] = set()
        self.task_subscriptions[task_id].add(client_id)
        logger.debug(f"Клиент {client_id} подписан на задачу {task_id}")
    
    def unsubscribe_from_task(self, client_id: str, task_id: str):
        if task_id in self.task_subscriptions:
            self.task_subscriptions[task_id].discard(client_id)
            if not self.task_subscriptions[task_id]:
                del self.task_subscriptions[task_id]
    
    async def send_task_update(self, task_id: str, data: dict):
        """Отправка обновления всем подписанным клиентам"""
        if task_id not in self.task_subscriptions:
            return
        
        disconnected_clients = []
        for client_id in self.task_subscriptions[task_id]:
            if client_id in self.active_connections:
                try:
                    await self.active_connections[client_id].send_json(data)
                except Exception as e:
                    logger.error(f"Ошибка отправки клиенту {client_id}: {e}")
                    disconnected_clients.append(client_id)
        
        for client_id in disconnected_clients:
            self.disconnect(client_id)
    
    async def broadcast(self, message: dict):
        """Рассылка всем подключенным клиентам"""
        disconnected = []
        for client_id, connection in self.active_connections.items():
            try:
                await connection.send_json(message)
            except:
                disconnected.append(client_id)
        
        for client_id in disconnected:
            self.disconnect(client_id)

manager = ConnectionManager()

# ----------------- Вспомогательные функции -----------------
async def cleanup_tasks_and_files(task_ids: List[str], file_paths: List[str]):
    """Отмена задач Celery и удаление файлов"""
    for t in task_ids:
        try:
            celery_app.control.revoke(t, terminate=True, signal='SIGKILL')
        except Exception as e:
            logger.warning(f"Ошибка отмены задачи {t}: {str(e)}")
    for f in file_paths:
        try:
            Path(f).unlink()
            logger.info(f"Удалён файл: {f}")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Ошибка удаления файла {f}: {str(e)}")


async def normalize_error(raw) -> dict:
    if isinstance(raw, dict) and 'exc_type' in raw:
        return {'type': raw['exc_type'],
                'message': str(raw.get('exc_message') or raw.get('exc_args', ''))}
    if isinstance(raw, dict) and 'type' in raw:
        return raw
    if isinstance(raw, BaseException):
        return {'type': type(raw).__name__, 'message': str(raw)}
    return {'type': 'Exception',
            'message': str(raw) if raw else 'Unknown error'}

# ----------------- Background task для мониторинга задач -----------------
async def monitor_task_status(task_id: str):
    """Фоновый мониторинг статуса задачи и отправка обновлений через WebSocket"""
    try:
        while True:
            task = celery_app.AsyncResult(task_id)
            state = task.state
            
            data = {"task_id": task_id, "type": "task_update"}
            
            if state == 'PENDING':
                data.update({
                    "status": "pending",
                    "progress": 0,
                    "message": "Задача в очереди..."
                })
            elif state == 'PROGRESS':
                info = task.info or {}
                data.update({
                    "status": "processing",
                    "progress": info.get('progress', 0),
                    "current_step": info.get('current_step', 0),
                    "total_steps": info.get('total_steps', 6),
                    "message": info.get('status', 'Обработка...'),
                    "filename": info.get('filename', '')
                })
            elif state == 'SUCCESS':
                result_data = task.result or {}
                data.update({
                    "status": "completed",
                    "progress": 100,
                    "result": result_data,
                    "message": "Обработка завершена"
                })
                await manager.send_task_update(task_id, data)
                break
            elif state == 'FAILURE':
                info = await normalize_error(task.info or {})
                data.update({
                    "status": "failed",
                    "error": info,
                    "message": "Ошибка обработки"
                })
                await manager.send_task_update(task_id, data)
                break
            elif state == 'REVOKED':
                data.update({
                    "status": "cancelled",
                    "message": "Задача отменена"
                })
                await manager.send_task_update(task_id, data)
                break
            
            await manager.send_task_update(task_id, data)
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Ошибка мониторинга задачи {task_id}: {e}")

# ----------------- Lifespan -----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_qdrant()
    create_document_collection()
    logger.info("✅ FastAPI startup completed")
    yield
    logger.info("🛑 FastAPI shutdown")

# ----------------- Инициализация FastAPI -----------------
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# ----------------- Middleware для оптимизации соединений -----------------
@app.middleware("http")
async def optimize_connection_headers(request: Request, call_next):
    """Оптимизированные заголовки для HTTP соединений"""
    response = await call_next(request)
    
    if request.url.path.startswith("/ws") or request.url.path.startswith("/task-status"):
        response.headers["Connection"] = "keep-alive"
        response.headers["Keep-Alive"] = "timeout=60, max=1000"
    else:
        response.headers["Connection"] = "close"
    
    return response

# ----------------- WebSocket эндпоинт -----------------
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """WebSocket соединение для real-time обновлений статуса задач"""
    await manager.connect(websocket, client_id)
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "subscribe":
                task_id = message.get("task_id")
                if task_id:
                    manager.subscribe_to_task(client_id, task_id)
                    asyncio.create_task(monitor_task_status(task_id))
                    await websocket.send_json({
                        "type": "subscribed",
                        "task_id": task_id,
                        "message": f"Подписка на задачу {task_id} активна"
                    })
            
            elif message.get("type") == "unsubscribe":
                task_id = message.get("task_id")
                if task_id:
                    manager.unsubscribe_from_task(client_id, task_id)
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "task_id": task_id
                    })
            
            elif message.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        manager.disconnect(client_id)
        logger.info(f"WebSocket клиент {client_id} отключен")
    except Exception as e:
        logger.error(f"WebSocket ошибка для клиента {client_id}: {e}")
        manager.disconnect(client_id)

# ----------------- Эндпоинты -----------------
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
async def css_styles():
    path = Path("server/semantic.html")
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return path.read_text(encoding="utf-8")

@app.get("/index", response_class=HTMLResponse)
async def index():
    path = Path("server/index.html")
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return path.read_text(encoding="utf-8")

# ----------------- Управление задачами Celery -----------------
@app.delete("/task-cancel/{task_id}")
async def cancel_task(task_id: str):
    """Отмена задачи Celery по ID"""
    try:
        celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
        logger.info(f"Task {task_id} cancelled")
        
        await manager.send_task_update(task_id, {
            "task_id": task_id,
            "type": "task_update",
            "status": "cancelled",
            "message": "Задача отменена пользователем"
        })
        
        return {"status": "cancelled", "task_id": task_id, "message": "Задача отменена"}
    except Exception as e:
        logger.exception(f"Ошибка отмены задачи {task_id}")
        raise HTTPException(status_code=500, detail=f"Ошибка отмены: {str(e)}")

@app.post("/tasks-cancel-batch")
async def cancel_tasks_batch(task_ids: List[str]):
    cancelled, errors = [], []
    for task_id in task_ids:
        try:
            celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
            cancelled.append(task_id)
            logger.info(f"Задача {task_id} отменена")
            
            await manager.send_task_update(task_id, {
                "task_id": task_id,
                "type": "task_update",
                "status": "cancelled",
                "message": "Задача отменена пользователем"
            })
        except Exception as e:
            errors.append({"task_id": task_id, "error": str(e)})
            logger.warning(f"Ошибка отмены {task_id}: {str(e)}")
    return {
        "status": "completed",
        "cancelled": cancelled,
        "cancelled_count": len(cancelled),
        "errors": errors,
        "errors_count": len(errors)
    }

@app.get("/tasks/active")
async def get_active_tasks():
    inspect = celery_app.control.inspect()
    active_tasks = inspect.active() or {}
    tasks_list = [
        {"task_id": t.get('id'), "name": t.get('name'), "worker": worker}
        for worker, tasks in active_tasks.items() if tasks
        for t in tasks
    ]
    return {"active_tasks": tasks_list, "count": len(tasks_list)}

# ----------------- Загрузка файлов -----------------
@app.post("/select-file")
async def select_file(request: Request, file: List[UploadFile] = File(...)):
    task_ids, uploaded_files, total_size, created_file_paths = [], [], 0, []

    try:
        for f in file:
            if await request.is_disconnected():
                await cleanup_tasks_and_files(task_ids, created_file_paths)
                logger.warning("Client disconnected during upload")
                raise HTTPException(status_code=499, detail="Client disconnected")

            try:
                contents = bytearray()
                chunk_size = 1024 * 1024
                
                try:
                    async with asyncio.timeout(60):
                        while chunk := await f.read(chunk_size):
                            if await request.is_disconnected():
                                await cleanup_tasks_and_files(task_ids, created_file_paths)
                                raise HTTPException(status_code=499, detail="Client disconnected")
                            contents.extend(chunk)
                except asyncio.TimeoutError:
                    await cleanup_tasks_and_files(task_ids, created_file_paths)
                    raise HTTPException(status_code=408, detail="Upload timeout")

                file_size = len(contents)
                total_size += file_size
                uploads_dir = Path("uploads")
                uploads_dir.mkdir(exist_ok=True)
                save_path = uploads_dir / f.filename
                save_path.write_bytes(contents)
                created_file_paths.append(str(save_path))

                task = generate_embedding.delay(f.filename)
                task_ids.append(task.id)
                
                uploaded_files.append({
                    "filename": f.filename,
                    "size": file_size,
                    "content_type": f.content_type,
                    "task_id": task.id
                })
                logger.info(f"Создана задача {task.id} для файла {f.filename}")

            finally:
                await f.close()

        return {
            "status": "accepted",
            "message": f"Принято {len(file)} файл(ов) в обработку",
            "files": uploaded_files,
            "task_ids": task_ids,
            "total_size": total_size,
            "count": len(file)
        }
    except HTTPException:
        raise
    except Exception as e:
        await cleanup_tasks_and_files(task_ids, created_file_paths)
        logger.exception("Error during file upload")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/select-folder")
async def select_folder(request: Request, file: List[UploadFile] = File(...), folder_name: Optional[str] = None):
    uploaded_files, file_paths, total_size = [], [], 0
    uploads_dir = Path("uploads")
    uploads_dir.mkdir(exist_ok=True)

    if not folder_name and file:
        folder_name = file[0].filename.split("/")[0] if "/" in file[0].filename else "uploaded_folder"
    folder_path = uploads_dir / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Начало загрузки папки: {folder_name}, файлов: {len(file)}")

    try:
        for f in file:
            if await request.is_disconnected():
                await cleanup_tasks_and_files([], file_paths)
                if folder_path.exists() and folder_path.is_dir():
                    shutil.rmtree(folder_path)
                logger.warning("Client disconnected during folder upload")
                raise HTTPException(status_code=499, detail="Client disconnected")

            try:
                try:
                    async with asyncio.timeout(60):
                        contents: bytes = await f.read()
                except asyncio.TimeoutError:
                    await cleanup_tasks_and_files([], file_paths)
                    raise HTTPException(status_code=408, detail="Upload timeout")
                
                file_size = len(contents)
                total_size += file_size

                save_path = uploads_dir / f.filename
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(contents)

                file_paths.append(str(save_path))
                uploaded_files.append({
                    "filename": f.filename,
                    "size": file_size,
                    "content_type": f.content_type,
                    "file_path": str(save_path),
                    "relative_path": f.filename
                })
                logger.info(f"Сохранен файл: {save_path}")
            finally:
                await f.close()

        task = generate_embedding_batch.delay(file_paths, folder_name)
        logger.info(f"Создана пакетная задача {task.id} для папки {folder_name}")
        return {
            "status": "accepted",
            "message": f"Принята папка '{folder_name}' с {len(file)} файлами в обработку",
            "folder_name": folder_name,
            "files": uploaded_files,
            "task_id": task.id,
            "total_size": total_size,
            "count": len(file),
            "mode": "batch"
        }

    except HTTPException:
        raise
    except Exception as e:
        await cleanup_tasks_and_files([], file_paths)
        logger.exception("Error during folder upload")
        raise HTTPException(status_code=500, detail=str(e))

# ----------------- Семантический поиск -----------------
@app.post("/message")
async def message_input(request: SearchRequest):
    """Поиск по семантическому запросу"""
    if not request.text or not request.text.strip():
        return {"status": "error", "message": "Текст запроса не может быть пустым", "results": []}

    try:
        search_result = reserch_similar_chunks(request.text)
        if not search_result:
            return {"status": "no_results", "message": "По вашему запросу ничего не найдено", "results": []}

        top_results = search_result[:5]
        formatted_results = [
            {
                "rank": i,
                "id": r.get('id', f'result_{i}'),
                "score": r['score'] * 100,
                "text": r['text'],
                "file_name": r.get('metadata', {}).get('file_name', 'Неизвестно'),
                "file_path": r.get('metadata', {}).get('file_path', 'Неизвестно'),
                "file_extension": r.get('metadata', {}).get('file_extension', 'Неизвестно'),
                "chunk_index": r.get('metadata', {}).get('chunk_index', 0)
            } for i, r in enumerate(top_results, 1)
        ]
        return {
            "status": "success",
            "message": f"Найдено результатов: {len(search_result)}",
            "count": len(formatted_results),
            "results": formatted_results
        }
    except Exception as e:
        logger.exception("Ошибка поиска")
        return {"status": "error", "message": f"Ошибка при выполнении поиска: {str(e)}", "results": []}

# ----------------- Статус задач (legacy HTTP endpoint) -----------------
@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Проверка статуса задачи по ID - legacy endpoint для обратной совместимости"""
    try:
        task = celery_app.AsyncResult(task_id)
        state = task.state

        if state == 'PENDING':
            return {
                "task_id": task_id, 
                "status": "pending", 
                "progress": 0, 
                "current_step": 1, 
                "total_steps": 6, 
                "message": "Задача в очереди..."
            }
        elif state == 'PROGRESS':
            info = task.info or {}
            return {
                "task_id": task_id,
                "status": "processing",
                "progress": info.get('progress', 0),
                "current_step": info.get('current_step', 0),
                "total_steps": info.get('total_steps', 6),
                "message": info.get('status', 'Обработка...'),
                "filename": info.get('filename', '')
            }
        elif state == 'SUCCESS':
            result_data = task.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "current_step": 6,
                "total_steps": 6,
                "result": result_data,
                "message": f"Обработка завершена"
            }
        elif state == 'FAILURE':
            error_info = normalize_error(task.info or {})
            return {"task_id": task_id, "state": "FAILURE", "error": error_info}
        else:
            return {"task_id": task_id, "status": state.lower(), "message": str(task.info)}

    except Exception as e:
        logger.exception("Ошибка получения статуса задачи")
        return {"task_id": task_id, "state": "ERROR", "error": f"Unexpected error: {str(e)}"}
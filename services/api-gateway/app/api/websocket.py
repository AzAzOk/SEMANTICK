# services/api-gateway/app/api/websocket.py
"""
WebSocket Manager для real-time обновлений статуса задач
"""

from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict
import logging
import json
import asyncio

logger = logging.getLogger(__name__)

# ==========================================
# WEBSOCKET CONNECTION MANAGER
# ==========================================

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


# Глобальный экземпляр менеджера
manager = ConnectionManager()


# ==========================================
# BACKGROUND TASK MONITORING
# ==========================================

async def monitor_task_status(task_id: str, celery_app):
    """Фоновый мониторинг статуса задачи и отправка обновлений через WebSocket"""
    
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


# ==========================================
# WEBSOCKET ENDPOINT
# ==========================================

async def websocket_endpoint(websocket: WebSocket, client_id: str, celery_app):
    """
    WebSocket endpoint для real-time обновлений статуса задач
    
    ВАЖНО: Это функция-обработчик, а не декоратор!
    Используется в main.py как:
    
    @app.websocket("/ws/{client_id}")
    async def ws_handler(websocket: WebSocket, client_id: str):
        await websocket_endpoint(websocket, client_id, celery_app)
    """
    await manager.connect(websocket, client_id)
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "subscribe":
                task_id = message.get("task_id")
                if task_id:
                    manager.subscribe_to_task(client_id, task_id)
                    asyncio.create_task(monitor_task_status(task_id, celery_app))
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
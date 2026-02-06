# services/api-gateway/app/api/websocket_redis.py
"""
WebSocket Manager с чтением статусов из Redis
"""

from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, Set
import logging
import json
import asyncio
from ..core.redis_client import task_status_manager

logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Менеджер WebSocket соединений
    Читает статусы задач из Redis и отправляет клиентам
    """
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.task_subscriptions: Dict[str, Set[str]] = {}  # task_id -> set of client_ids
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}  # task_id -> asyncio Task
    
    async def connect(self, websocket: WebSocket, client_id: str):
        """Подключение нового WebSocket клиента"""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"🔌 WebSocket client connected: {client_id}")
    
    def disconnect(self, client_id: str):
        """Отключение WebSocket клиента"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        
        # Удаляем клиента из всех подписок
        for task_id in list(self.task_subscriptions.keys()):
            if client_id in self.task_subscriptions[task_id]:
                self.task_subscriptions[task_id].discard(client_id)
                
                # Если подписчиков не осталось - останавливаем мониторинг
                if not self.task_subscriptions[task_id]:
                    del self.task_subscriptions[task_id]
                    if task_id in self.monitoring_tasks:
                        self.monitoring_tasks[task_id].cancel()
                        del self.monitoring_tasks[task_id]
        
        logger.info(f"🔌 WebSocket client disconnected: {client_id}")
    
    def subscribe_to_task(self, client_id: str, task_id: str):
        """Подписка клиента на обновления задачи"""
        if task_id not in self.task_subscriptions:
            self.task_subscriptions[task_id] = set()
        
        self.task_subscriptions[task_id].add(client_id)
        
        # Запускаем мониторинг задачи если ещё не запущен
        if task_id not in self.monitoring_tasks:
            task = asyncio.create_task(self._monitor_task(task_id))
            self.monitoring_tasks[task_id] = task
        
        logger.debug(f"📡 Client {client_id} subscribed to task {task_id}")
    
    def unsubscribe_from_task(self, client_id: str, task_id: str):
        """Отписка клиента от обновлений задачи"""
        if task_id in self.task_subscriptions:
            self.task_subscriptions[task_id].discard(client_id)
            
            # Останавливаем мониторинг если подписчиков нет
            if not self.task_subscriptions[task_id]:
                del self.task_subscriptions[task_id]
                if task_id in self.monitoring_tasks:
                    self.monitoring_tasks[task_id].cancel()
                    del self.monitoring_tasks[task_id]
        
        logger.debug(f"📡 Client {client_id} unsubscribed from task {task_id}")
    
    async def _monitor_task(self, task_id: str):
        """
        Фоновый мониторинг задачи
        Читает статус из Redis и отправляет подписанным клиентам
        """
        try:
            last_status = None
            
            while True:
                # Читаем статус из Redis
                status_data = await task_status_manager.get_task_status(task_id)
                
                if not status_data:
                    logger.warning(f"⚠️ Task {task_id} not found in Redis")
                    await asyncio.sleep(1)
                    continue
                
                # Отправляем обновление только если статус изменился
                if status_data != last_status:
                    await self._send_task_update(task_id, status_data)
                    last_status = status_data
                
                # Если задача завершена - останавливаем мониторинг
                if status_data.get('status') in ['completed', 'failed', 'cancelled']:
                    logger.info(f"✅ Task {task_id} finished with status: {status_data.get('status')}")
                    await self._send_task_update(task_id, status_data)
                    break
                
                await asyncio.sleep(0.5)  # Проверяем каждые 500ms
                
        except asyncio.CancelledError:
            logger.debug(f"🛑 Monitoring cancelled for task {task_id}")
        except Exception as e:
            logger.error(f"❌ Error monitoring task {task_id}: {e}")
    
    async def _send_task_update(self, task_id: str, status_data: dict):
        """Отправка обновления всем подписанным клиентам"""
        if task_id not in self.task_subscriptions:
            return
        
        # Формируем сообщение
        message = {
            "type": "task_update",
            **status_data
        }
        
        # Отправляем всем подписанным клиентам
        disconnected_clients = []
        
        for client_id in self.task_subscriptions[task_id]:
            if client_id in self.active_connections:
                try:
                    await self.active_connections[client_id].send_json(message)
                except Exception as e:
                    logger.error(f"❌ Failed to send to client {client_id}: {e}")
                    disconnected_clients.append(client_id)
        
        # Отключаем проблемных клиентов
        for client_id in disconnected_clients:
            self.disconnect(client_id)
    
    async def broadcast(self, message: dict):
        """Рассылка сообщения всем подключенным клиентам"""
        disconnected = []
        
        for client_id, connection in self.active_connections.items():
            try:
                await connection.send_json(message)
            except:
                disconnected.append(client_id)
        
        for client_id in disconnected:
            self.disconnect(client_id)


# Глобальный экземпляр менеджера
ws_manager = WebSocketManager()


async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """
    WebSocket endpoint для real-time обновлений
    
    Использование в main.py:
    @app.websocket("/ws/{client_id}")
    async def ws_handler(websocket: WebSocket, client_id: str):
        await websocket_endpoint(websocket, client_id)
    """
    await ws_manager.connect(websocket, client_id)
    
    try:
        while True:
            # Получаем сообщение от клиента
            data = await websocket.receive_text()
            message = json.loads(data)
            
            message_type = message.get("type")
            
            if message_type == "subscribe":
                # Подписка на задачу
                task_id = message.get("task_id")
                if task_id:
                    ws_manager.subscribe_to_task(client_id, task_id)
                    await websocket.send_json({
                        "type": "subscribed",
                        "task_id": task_id,
                        "message": f"Subscribed to task {task_id}"
                    })
            
            elif message_type == "unsubscribe":
                # Отписка от задачи
                task_id = message.get("task_id")
                if task_id:
                    ws_manager.unsubscribe_from_task(client_id, task_id)
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "task_id": task_id
                    })
            
            elif message_type == "ping":
                # Проверка соединения
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        ws_manager.disconnect(client_id)
        logger.info(f"WebSocket client {client_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
        ws_manager.disconnect(client_id)
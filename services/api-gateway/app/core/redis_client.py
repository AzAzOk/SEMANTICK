# services/api-gateway/app/core/redis_client.py
"""
Redis Client для хранения и чтения статусов задач
"""

import redis.asyncio as aioredis
import json
import logging
from typing import Dict, Any, Optional
from .config import settings

logger = logging.getLogger(__name__)


class TaskStatusManager:
    """
    Менеджер статусов задач в Redis
    
    Schema:
    task:{task_id}:status -> JSON с полями:
        - status: pending|processing|completed|failed
        - progress: 0-100
        - current_step: 1-6
        - total_steps: 6
        - message: описание
        - filename: имя файла
        - error: объект ошибки (если failed)
        - result: результат (если completed)
    """
    
    def __init__(self):
        self.redis = None
        self.ttl = 3600  # 1 час
    
    async def connect(self):
        """Подключение к Redis"""
        try:
            self.redis = await aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            logger.info("✅ Redis connection established")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
    
    async def create_task(self, task_id: str, filename: str) -> bool:
        """
        Создание новой задачи со статусом pending
        
        Args:
            task_id: Уникальный ID задачи
            filename: Имя файла
        """
        try:
            task_data = {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "current_step": 0,
                "total_steps": 6,
                "message": "Задача в очереди...",
                "filename": filename
            }
            
            key = f"task:{task_id}:status"
            await self.redis.setex(
                key,
                self.ttl,
                json.dumps(task_data)
            )
            
            logger.info(f"📝 Created task status: {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create task {task_id}: {e}")
            return False
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Получение статуса задачи
        
        Args:
            task_id: ID задачи
            
        Returns:
            Словарь со статусом или None если не найдено
        """
        try:
            key = f"task:{task_id}:status"
            data = await self.redis.get(key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get task status {task_id}: {e}")
            return None
    
    async def update_task_status(
        self,
        task_id: str,
        status: str = None,
        progress: int = None,
        current_step: int = None,
        message: str = None,
        error: Dict = None,
        result: Dict = None
    ) -> bool:
        """
        Обновление статуса задачи
        
        Args:
            task_id: ID задачи
            status: Новый статус
            progress: Прогресс 0-100
            current_step: Текущий шаг
            message: Сообщение о текущем состоянии
            error: Объект ошибки
            result: Результат выполнения
        """
        try:
            # Получаем текущие данные
            current_data = await self.get_task_status(task_id)
            if not current_data:
                logger.warning(f"Task {task_id} not found, cannot update")
                return False
            
            # Обновляем только переданные поля
            if status is not None:
                current_data['status'] = status
            if progress is not None:
                current_data['progress'] = progress
            if current_step is not None:
                current_data['current_step'] = current_step
            if message is not None:
                current_data['message'] = message
            if error is not None:
                current_data['error'] = error
            if result is not None:
                current_data['result'] = result
            
            # Сохраняем обновленные данные
            key = f"task:{task_id}:status"
            await self.redis.setex(
                key,
                self.ttl,
                json.dumps(current_data)
            )
            
            logger.debug(f"📝 Updated task {task_id}: {status or 'no status change'}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update task {task_id}: {e}")
            return False
    
    async def delete_task(self, task_id: str) -> bool:
        """Удаление задачи из Redis"""
        try:
            key = f"task:{task_id}:status"
            await self.redis.delete(key)
            logger.info(f"🗑️ Deleted task: {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete task {task_id}: {e}")
            return False
    
    async def close(self):
        """Закрытие соединения с Redis"""
        try:
            if self.redis:
                await self.redis.close()
                logger.info("🔌 Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")


# Глобальный экземпляр менеджера
task_status_manager = TaskStatusManager()
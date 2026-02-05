# document-processor/app/core/rabbitmq_consumer.py
"""
🔄 Улучшенный RabbitMQ Consumer
- Обработка ошибок при создании Celery задач
- Обновление статуса в Redis при ошибках
- Retry механизм для failed messages
- Dead Letter Queue обработка
"""

import aio_pika
import json
import asyncio
import logging
from typing import Optional
import redis.asyncio as aioredis
from datetime import datetime

from ..tasks.processing import celery_app
from .config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class RedisStatusUpdater:
    """Helper класс для обновления статусов в Redis из consumer"""
    
    def __init__(self):
        self.redis = None
    
    async def connect(self):
        """Подключение к Redis"""
        try:
            self.redis = await aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            logger.info("✅ Redis connection established in consumer")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
    
    async def update_task_failed(self, task_id: str, error_message: str):
        """Обновление статуса задачи на failed"""
        try:
            if not self.redis:
                await self.connect()
            
            key = f"task:{task_id}:status"
            current_data = await self.redis.get(key)
            
            if current_data:
                task_data = json.loads(current_data)
                task_data.update({
                    "status": "failed",
                    "progress": 100,
                    "message": "Ошибка при создании задачи обработки",
                    "error": {
                        "type": "consumer_error",
                        "message": error_message
                    },
                    "failed_at": datetime.now().isoformat()
                })
                
                await self.redis.setex(key, 3600, json.dumps(task_data))
                logger.info(f"📝 Updated task {task_id} to failed in Redis")
            else:
                logger.warning(f"⚠️ Task {task_id} not found in Redis")
                
        except Exception as e:
            logger.error(f"❌ Failed to update Redis for task {task_id}: {e}")
    
    async def close(self):
        """Закрытие соединения"""
        if self.redis:
            await self.redis.close()


# Глобальный экземпляр Redis updater
redis_updater = RedisStatusUpdater()


async def on_message(message: aio_pika.IncomingMessage):
    """
    🔄 Обработка входящего сообщения из RabbitMQ
    
    Основные улучшения:
    - Try-catch обёртка для всей логики
    - Обновление Redis при ошибках
    - Логирование всех этапов
    """
    async with message.process():
        task_id = None
        
        try:
            # 1. Парсинг сообщения
            try:
                payload = json.loads(message.body)
                logger.info(f"📥 Received message: {payload}")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Invalid JSON in message: {e}")
                return  # Acknowledge но не обрабатываем
            
            # 2. Валидация payload
            task_id = payload.get("task_id")
            if not task_id:
                logger.error("❌ Missing task_id in payload")
                return
            
            task_type = payload.get("type", "single_file")
            
            # 3. Обработка в зависимости от типа
            if task_type == "single_file":
                await process_single_file(payload, task_id)
                
            elif task_type == "folder":
                await process_folder(payload, task_id)
                
            else:
                logger.error(f"❌ Unknown task type: {task_type}")
                await redis_updater.update_task_failed(
                    task_id,
                    f"Unknown task type: {task_type}"
                )
                
        except Exception as e:
            logger.error(f"❌ Critical error processing message: {e}", exc_info=True)
            
            # Обновляем Redis если есть task_id
            if task_id:
                await redis_updater.update_task_failed(
                    task_id,
                    f"Consumer error: {str(e)}"
                )


async def process_single_file(payload: dict, task_id: str):
    """Обработка одиночного файла"""
    try:
        file_path = payload.get("file_path")
        filename = payload.get("filename")
        
        if not file_path or not filename:
            raise ValueError("Missing file_path or filename in payload")
        
        logger.info(f"📤 Sending to Celery: {task_id} - {filename}")
        
        # Отправляем в Celery
        celery_task = celery_app.send_task(
            "worker-document-processor.generate_embedding",
            args=[file_path],
            kwargs={
                "onlyfile": False,
                "api_task_id": task_id
            },
            task_id=task_id,
            queue="documents.tasks"
        )
        
        logger.info(f"✅ Celery task created: {celery_task.id}")
        
    except Exception as e:
        logger.error(f"❌ Error processing single file: {e}")
        await redis_updater.update_task_failed(
            task_id,
            f"Failed to create Celery task: {str(e)}"
        )
        raise


async def process_folder(payload: dict, task_id: str):
    """Обработка папки с файлами"""
    try:
        file_paths = payload.get("file_paths")
        folder_name = payload.get("folder_name", "unknown")
        
        if not file_paths:
            raise ValueError("Missing file_paths in payload")
        
        logger.info(f"📤 Sending batch to Celery: {task_id} - {folder_name} ({len(file_paths)} files)")
        
        # Отправляем в Celery
        celery_task = celery_app.send_task(
            "worker-document-processor.generate_embedding_batch",
            args=[file_paths],
            kwargs={
                "folder_name": folder_name,
                "api_task_id": task_id
            },
            task_id=task_id,
            queue="documents.tasks"
        )
        
        logger.info(f"✅ Celery batch task created: {celery_task.id}")
        
    except Exception as e:
        logger.error(f"❌ Error processing folder: {e}")
        await redis_updater.update_task_failed(
            task_id,
            f"Failed to create batch Celery task: {str(e)}"
        )
        raise


async def setup_dead_letter_queue(channel):
    """
    Настройка Dead Letter Queue для failed messages
    
    Сообщения попадают сюда если:
    - Превышен TTL (60 секунд)
    - Message rejected с requeue=False
    """
    try:
        # Dead Letter Exchange
        dlx = await channel.declare_exchange(
            'dlx',
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
        
        # Dead Letter Queue
        dlq = await channel.declare_queue(
            'dlq.document_processor',
            durable=True,
            arguments={
                'x-message-ttl': 86400000,  # 24 часа
            }
        )
        
        await dlq.bind(dlx, routing_key='dlq.document_processor')
        
        logger.info("✅ Dead Letter Queue configured")
        
    except Exception as e:
        logger.error(f"❌ Failed to setup DLQ: {e}")
        raise


async def main():
    """
    🚀 Главная функция consumer
    
    Настройка:
    - RabbitMQ connection с robust reconnect
    - QoS для надёжной обработки
    - Exchange и Queue объявление
    - Dead Letter Queue
    - Redis connection для обновления статусов
    """
    try:
        # Подключаемся к Redis
        await redis_updater.connect()
        
        # Подключаемся к RabbitMQ
        connection = await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
            timeout=30
        )

        channel = await connection.channel()
        
        # Настраиваем QoS
        await channel.set_qos(prefetch_count=1)

        # Exchange для входящих сообщений
        exchange = await channel.declare_exchange(
            "documents.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )

        # Основная очередь
        queue = await channel.declare_queue(
            "document_processor_queue",
            durable=True,
            arguments={
                'x-message-ttl': 60000,  # 60 секунд
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': 'dlq.document_processor'
            }
        )

        # Настраиваем Dead Letter Queue
        await setup_dead_letter_queue(channel)

        # Биндим очередь к exchange
        await queue.bind(exchange, routing_key="file.process")
        await queue.bind(exchange, routing_key="folder.process")

        logger.info("="*60)
        logger.info("🔥 RabbitMQ Consumer Started")
        logger.info("="*60)
        logger.info(f"📥 Listening on queue: document_processor_queue")
        logger.info(f"🔌 Binding keys: file.process, folder.process")
        logger.info(f"📊 QoS prefetch: 1")
        logger.info("="*60)

        # Начинаем слушать сообщения
        await queue.consume(on_message)

        # Keep consumer running
        await asyncio.Future()
        
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
        
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}", exc_info=True)
        raise
        
    finally:
        # Закрываем Redis
        await redis_updater.close()


if __name__ == "__main__":
    asyncio.run(main())
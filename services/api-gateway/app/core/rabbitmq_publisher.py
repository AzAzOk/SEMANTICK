# services/api-gateway/app/core/rabbitmq_publisher.py
"""
RabbitMQ Publisher для отправки асинхронных задач
"""

import aio_pika
import json
import logging
from .config import settings

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    """
    Publisher для отправки задач в RabbitMQ
    
    Exchanges:
    - documents.events: для задач обработки документов
    - embeddings: для задач создания эмбеддингов (будущее)
    """
    
    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchanges = {}
    
    async def connect(self):
        """Установка соединения с RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                timeout=10
            )
            self.channel = await self.connection.channel()
            
            # Объявляем exchange для документов
            self.exchanges['documents.events'] = await self.channel.declare_exchange(
                'documents.events',
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            logger.info("✅ RabbitMQ connection established")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            raise
    
    async def publish_file_task(self, task_id: str, file_path: str, filename: str) -> bool:
        """
        Публикация задачи на обработку файла
        
        Args:
            task_id: Уникальный ID задачи
            file_path: Путь к файлу в файловой системе
            filename: Имя файла
            
        Returns:
            True если успешно опубликовано
        """
        try:
            message_body = {
                "task_id": task_id,
                "file_path": file_path,
                "filename": filename,
                "type": "single_file"
            }
            
            message = aio_pika.Message(
                body=json.dumps(message_body).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type='application/json'
            )
            
            await self.exchanges['documents.events'].publish(
                message,
                routing_key='file.process'
            )
            
            logger.info(f"📤 Published file task: {task_id} - {filename}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish file task {task_id}: {e}")
            return False
    
    async def publish_folder_task(
        self, 
        task_id: str, 
        file_paths: list[str], 
        folder_name: str
    ) -> bool:
        """
        Публикация задачи на обработку папки с файлами
        
        Args:
            task_id: Уникальный ID задачи
            file_paths: Список путей к файлам
            folder_name: Имя папки
            
        Returns:
            True если успешно опубликовано
        """
        try:
            message_body = {
                "task_id": task_id,
                "file_paths": file_paths,
                "folder_name": folder_name,
                "type": "folder"
            }
            
            message = aio_pika.Message(
                body=json.dumps(message_body).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type='application/json'
            )
            
            await self.exchanges['documents.events'].publish(
                message,
                routing_key='folder.process'
            )
            
            logger.info(f"📤 Published folder task: {task_id} - {folder_name} ({len(file_paths)} files)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish folder task {task_id}: {e}")
            return False
    
    async def close(self):
        """Закрытие соединения с RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("🔌 RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")


# Глобальный экземпляр publisher
publisher = RabbitMQPublisher()

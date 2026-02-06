"""
RabbitMQ Publisher для отправки embedding-событий
"""

import aio_pika
import json
import logging
from .config import settings

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    """
    Publisher для отправки событий в RabbitMQ

    Exchanges:
    - documents.events: события обработки документов и embeddings
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

            # Exchange для document / embedding событий
            self.exchanges['documents.events'] = await self.channel.declare_exchange(
                'documents.events',
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )

            logger.info("✅ RabbitMQ connection established (document-processor)")

        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            raise

    async def publish_embedding_task(
        self,
        task_id: str,
        file_name: str,
        file_extension: str,
        chunks: list[dict]
    ) -> bool:
        """
        Публикация события для генерации embeddings

        Args:
            task_id: ID основной задачи
            file_name: Имя файла
            file_extension: Расширение файла
            chunks: Результат DocumentChunker.uniter (points_added)

        Returns:
            True если успешно опубликовано
        """
        try:
            message_body = {
                "task_id": task_id,
                "type": "embedding",
                "document": {
                    "file_name": file_name,
                    "file_extension": file_extension,
                    "chunks": chunks
                }
            }

            message = aio_pika.Message(
                body=json.dumps(message_body, ensure_ascii=False).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )

            await self.exchanges['documents.events'].publish(
                message,
                routing_key="embedding.process"
            )

            logger.info(
                f"📤 Published embedding task: {task_id} "
                f"({len(chunks)} chunks, file={file_name})"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish embedding task {task_id}: {e}")
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

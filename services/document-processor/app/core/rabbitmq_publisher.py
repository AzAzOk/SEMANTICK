import aio_pika
import json
import logging
from .config import settings

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    """
    Publisher для отправки задач в RabbitMQ
    
    Exchanges:
    - pub-in-worker: для задач обработки документов
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
            self.exchanges['pub-in-worker'] = await self.channel.declare_exchange(
                'pub-in-worker',
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            logger.info("✅ RabbitMQ connection established")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            raise

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

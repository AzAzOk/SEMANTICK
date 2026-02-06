import aio_pika
import json
import logging
from .config import settings
from ..handlers.embedding_handler import handle_embedding_event
from .config import settings

logger = logging.getLogger(__name__)


class RabbitMQConsumer:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
            timeout=10
        )
        self.channel = await self.connection.channel()

        exchange = await self.channel.declare_exchange(
            "documents.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )

        self.queue = await self.channel.declare_queue(
            "embedding.process",
            durable=True
        )

        await self.queue.bind(
            exchange=exchange,
            routing_key="embedding.process"
        )

        logger.info("✅ embedding-processor connected to RabbitMQ")

    async def consume(self):
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        payload = json.loads(message.body.decode())
                        await handle_embedding_event(payload)
                    except Exception as e:
                        logger.error(f"❌ Error processing embedding message: {e}")
                        raise

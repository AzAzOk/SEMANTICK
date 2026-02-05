import asyncio
import logging
from .core.rabbitmq_consumer import RabbitMQConsumer

logging.basicConfig(level=logging.INFO)


async def main():
    consumer = RabbitMQConsumer()
    await consumer.connect()
    await consumer.consume()


if __name__ == "__main__":
    asyncio.run(main())
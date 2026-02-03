# document-processor/app/core/rabbitmq_consumer.py
import aio_pika
import json
import asyncio
import logging
from ..tasks.processing import celery_app
from .config import settings
from .rabbitmq_publisher import publisher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def on_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body)
            logger.info(f"📥 Received message: {payload}")
            
            # Определяем тип задачи
            task_type = payload.get("type", "single_file")
            
            if task_type == "single_file":
                task_id = payload["task_id"]
                file_path = payload["file_path"]
                filename = payload["filename"]
                
                logger.info(f"📤 Sending to Celery: {task_id} - {filename}")
                
                # Отправляем в Celery с правильным routing_key
                celery_task = celery_app.send_task(
                    "worker-document-processor.generate_embedding",
                    args=[file_path],
                    kwargs={
                        "onlyfile": False,
                        "api_task_id": task_id},  # Передаем task_id из API Gateway
                    task_id=task_id,  # Используем тот же task_id
                    queue="documents.tasks"  # Важно: та же очередь что в docker-compose
                )
                
                logger.info(f"✅ Celery task created: {celery_task.id}")
                
            elif task_type == "folder":
                task_id = payload["task_id"]
                file_paths = payload["file_paths"]
                folder_name = payload.get("folder_name", "unknown")
                
                logger.info(f"📤 Sending batch to Celery: {task_id} - {folder_name}")
                
                celery_task = celery_app.send_task(
                    "worker-document-processor.generate_embedding_batch",
                    args=[file_paths],
                    kwargs={"folder_name": folder_name, "api_task_id": task_id},
                    task_id=task_id,
                    queue="documents.tasks"
                )
                
                logger.info(f"✅ Celery batch task created: {celery_task.id}")
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")


async def main():
    try:
        connection = await aio_pika.connect_robust(
            settings.RABBITMQ_URL
        )

        channel = await connection.channel()
        
        # Настраиваем QoS для надежной обработки
        await channel.set_qos(prefetch_count=1)

        # Exchange для входящих сообщений
        exchange = await channel.declare_exchange(
            "documents.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )

        # Очередь для document-processor
        queue = await channel.declare_queue(
            "document_processor_queue",
            durable=True,
            arguments={
                'x-message-ttl': 60000,
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': 'dlq.document_processor'
            }
        )

        # Dead letter exchange для обработки неудачных сообщений
        dlx = await channel.declare_exchange(
            'dlx',
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
        dlq = await channel.declare_queue(
            'dlq.document_processor',
            durable=True
        )
        await dlq.bind(dlx, routing_key='dlq.document_processor')

        # Биндим нашу основную очередь
        await queue.bind(exchange, routing_key="file.process")
        await queue.bind(exchange, routing_key="folder.process")

        logger.info("📥 Waiting for messages on document_processor_queue...")
        logger.info(f"📌 Binding keys: file.process, folder.process")

        await queue.consume(on_message)

        # Keep consumer running
        await asyncio.Future()
        
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
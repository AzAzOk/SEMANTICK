import shutil
from celery import Celery
from pathlib import Path
from ..core.chanking.text_spliter import TextSplitter
from ..core.parsers_system import ParserManager
from ..core.chanking import DocumentChunker, BusinessMetadata
from ..core.config import settings
from ..core.rabbitmq_publisher import publisher
import json
import logging
import redis.asyncio as aioredis
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

celery_app = Celery(
    'app.tasks.processing',
    backend=settings.REDIS_URL
)

# Настройка конфигурации Celery
celery_app.conf.update(
    # Базовая конфигурация
    result_expires=settings.RESULT_EXP,
    task_track_started=settings.T_TRACK_STR,
    task_soft_time_limit=settings.T_SOFT_TIME_LIMIT,
    task_time_limit=settings.T_TIME_LIM,
    task_acks_late=settings.T_ACKS_LATE,
    worker_prefetch_multiplier=settings.W_PREFETCH_MULT,
    task_reject_on_worker_lost=settings.T_REJECT_ON_W_LOST,
    worker_max_tasks_per_child=settings.W_MAX_T_PER_CHILD,
    
    # Имя задачи должно совпадать
    task_default_queue='documents.tasks',
    
    # Импорт задач
    imports=['app.tasks.processing'],
    
    # Имена задач
    task_routes={
        'app.tasks.processing.generate_embedding': {
            'queue': 'documents.tasks'
        },
        'app.tasks.processing.generate_embedding_batch': {
            'queue': 'documents.tasks'
        }
    },
    
    broker_transport_options={
        'visibility_timeout': settings.BTO_VT,
        'socket_keepalive': settings.BTO_SK,
        'socket_keepalive_options': {
            1: 1,
            2: 1,
            3: 5,
        },
    },
)


async def update_task_status_async(task_id: str, **updates):
    """Асинхронное обновление статуса задачи в Redis"""
    try:
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True
        )
        
        key = f"task:{task_id}:status"
        current_data = await redis_client.get(key)
        
        if current_data:
            task_data = json.loads(current_data)
            task_data.update(updates)
            task_data['last_updated'] = datetime.now().isoformat()
            
            await redis_client.setex(key, 3600, json.dumps(task_data))
            logger.info(f"Updated Redis task status: {task_id} - {updates.get('status', 'no status')}")
        else:
            logger.warning(f"Task {task_id} not found in Redis")
        
        await redis_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to update Redis task status {task_id}: {e}")
        return False

def update_task_status_sync(task_id: str, **updates):
    """Синхронная обёртка для обновления статуса в Redis"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(update_task_status_async(task_id, **updates))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"Sync wrapper error: {e}")
        return False

def _cleanup_file(filename: str, worker_name: str = "") -> None:
    """Универсальная функция для удаления файла с логированием"""
    try:
        if filename.startswith("uploads") or filename.startswith("uploads\\"):
            file_path = Path(filename)
        else:
            UPLOADS = Path(settings.UPLOAD_DIR)
            file_path = UPLOADS / filename
        
        if file_path.exists():
            file_path.unlink()
            logger.info(f"[{worker_name}] Файл удалён: {file_path}")
            return True
        else:
            logger.warning(f"[{worker_name}] Файл не найден для удаления: {file_path}")
            return False
    except Exception as e:
        logger.error(f"[{worker_name}] Ошибка удаления файла {filename}: {e}")
        return False

@celery_app.task(bind=True, name='worker-document-processor.generate_embedding')
def generate_embedding(self, filename: str, onlyfile: bool = False, api_task_id: str = None):
    """Обработка файла с отслеживанием прогресса и обновлением статуса в Redis"""
    
    worker_name = self.request.hostname
    filenames = Path(filename)
    task_id = api_task_id or self.request.id
    
    logger.info(f"[{worker_name}] Starting task {task_id} for file: {filename}")
    
    # Определяем display имя файла
    dispach = filename
    if onlyfile:
        parts = filename.split('\\')
        dispach = '\\'.join(parts[2:]) if len(parts) > 2 else filename
    
    # 1. Обновляем статус: начата обработка
    if task_id and not task_id.startswith('celery-'):
        update_task_status_sync(
            task_id,
            status="processing",
            progress=0,
            current_step=1,
            total_steps=6,
            message="Начинается обработка файла...",
            filename=dispach or filenames.name,
            worker=worker_name,
            started_at=datetime.now().isoformat()
        )
    
    # Проверяем формат файла
    ext = filenames.suffix.lower()
    if ext not in settings.SUPPORTED_EXTENSIONS:
        _cleanup_file(filename, worker_name)
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="failed",
                progress=100,
                current_step=1,
                total_steps=6,
                message=f"Неподдерживаемый формат файла: {ext}",
                error={
                    "type": "unsupported_format",
                    "message": f"Формат {ext} не поддерживается"
                },
                completed_at=datetime.now().isoformat()
            )
        
        return {
            "status": "skipped",
            "reason": "unsupported_format",
            "file_name": filename,
            "message": f"Файл {filename} пропущен (неподдерживаемый формат {ext})",
            "worker": worker_name
        }

    try:
        # 2. Проверка файла на дубликаты (шаг 1)
        logger.info(f"[{worker_name}] Проверка файла {dispach or filename}")
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                current_step=2,
                progress=15,
                status="processing",
                message="Проверка файла на дубликаты..."
            )
        
        # TODO: Добавить проверку дубликатов из базы данных
        # if reserch_file_name(dispach or filename):
        #     _cleanup_file(filename, worker_name)
        #     update_task_status_sync(task_id, status="skipped", reason="already_exists")
        #     return {"status": "skipped", "reason": "already_exists"}

        # 3. Поиск файла в файловой системе (шаг 2)
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                current_step=3,
                progress=30,
                status="processing", 
                message="Поиск файла в файловой системе..."
            )
        
        if filename.startswith("uploads") or filename.startswith("uploads\\"):
            file_path = Path(filename)
        else:
            UPLOADS = Path(settings.UPLOAD_DIR)
            file_path = UPLOADS / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"Файл {filename} не найден по пути: {file_path}")
        
        logger.info(f"[{worker_name}] Файл найден: {file_path.resolve()}")
        
        # 4. Парсинг файла (шаг 3)
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                current_step=4,
                progress=45,
                status="processing",
                message="Парсинг содержимого файла..."
            )
        
        manager = ParserManager()
        splitter = TextSplitter()
        data_metadata = BusinessMetadata()
        
        ext = manager._parser_extension(str(file_path))
        logger.info(f"[{worker_name}] Расширение файла: {ext}")
        
        parser_class = manager._find_parser_in_registry(ext)
        if parser_class is None:
            raise ValueError(f"Парсер для расширения .{ext} не найден")
        
        result_parser = manager._ransfer_selected_parser(str(file_path), parser_class)
        if result_parser is None:
            raise ValueError(f"Ошибка парсинга файла {filename}")
        
        if len(result_parser.text) == 0:
            raise ValueError(f"Файл {filename} пустой")
        
        text_length = len(result_parser.text)
        logger.info(f"[{worker_name}] Парсинг завершен: {text_length} символов")

        # 5. Разбиение на чанки (шаг 4)
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                current_step=5,
                progress=60,
                status="processing",
                message="Разбиение текста на смысловые чанки..."
            )
        
        chunks = splitter.split_text(result_parser.text)
        chunks_count = len(chunks)
        
        if chunks_count == 0:
            raise ValueError("Не удалось создать чанки из текста")
        
        logger.info(f"[{worker_name}] Создано чанков: {chunks_count}")

        # 6. Создание метаданных (шаг 5)
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                current_step=6,
                progress=75,
                status="processing",
                message="Формирование метаданных документа..."
            )

        metaDocument = DocumentChunker(chunks)
        result_uniter = metaDocument.uniter(
            result_parser.metadata,
            str(file_path),
            manager._file_name(str(file_path)),
            ext,
            data_metadata
        )


        async def _publish_embedding():
            await publisher.connect()
            await publisher.publish_embedding_task(
                task_id=task_id,
                file_name=filenames.name,
                file_extension=ext,
                chunks=result_uniter
            )
            await publisher.close()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_publish_embedding())
        loop.close()

        
        logger.info(f"[{worker_name}] Метаданные созданы")
        
        # 7. Сохранение в векторную БД (шаг 6) - ЗАКОММЕНТИРОВАНО
        # if task_id and not task_id.startswith('celery-'):
        #     update_task_status_sync(
        #         task_id,
        #         progress=90,
        #         status="processing",
        #         message="Сохранение в векторную базу данных..."
        #     )
        
        # Очистка файла
        _cleanup_file(filename, worker_name)

        # Формируем результат
        result = {
            "status": "success",
            "filename": filename,
            "display_name": dispach or filenames.name,
            "worker": worker_name,
            "text_length": text_length,
            "chunks_count": chunks_count,
            "points_added": result_uniter,
            "file_extension": ext,
            "processed_at": datetime.now().isoformat()
        }
        
        # 8. Финальное обновление статуса
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="completed",
                progress=100,
                current_step=6,
                total_steps=6,
                message="Обработка файла успешно завершена!",
                result=result,
                completed_at=datetime.now().isoformat()
            )
        
        logger.info(f"[{worker_name}] Файл {filename} успешно обработан!")
        
        return result
        
    except FileNotFoundError as e:
        error_msg = str(e)
        logger.error(f"[{worker_name}] Файл не найден: {error_msg}")
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="failed",
                progress=100,
                message=f"Файл не найден: {error_msg}",
                error={
                    "type": "file_not_found",
                    "message": error_msg
                },
                completed_at=datetime.now().isoformat()
            )
        
        raise
        
    except ValueError as e:
        error_msg = str(e)
        logger.error(f"[{worker_name}] Ошибка валидации: {error_msg}")
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="failed",
                progress=100,
                message=f"Ошибка обработки: {error_msg}",
                error={
                    "type": "validation_error",
                    "message": error_msg
                },
                completed_at=datetime.now().isoformat()
            )
        
        raise
        
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        logger.error(f"[{worker_name}] {error_msg}")
        
        # Очистка файла в случае ошибки
        _cleanup_file(filename, worker_name)
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="failed",
                progress=100,
                message=f"Критическая ошибка: {error_msg}",
                error={
                    "type": "unexpected_error",
                    "message": error_msg,
                    "exception_type": type(e).__name__
                },
                completed_at=datetime.now().isoformat()
            )
        
        raise

@celery_app.task(bind=True, name='app.tasks.processing.generate_embedding_batch')
def generate_embedding_batch(self, file_paths: list[str], folder_name: str = "", api_task_id: str = None):
    """Обработка нескольких файлов из папки с общим отслеживанием прогресса"""
    
    worker_name = self.request.hostname
    task_id = api_task_id or self.request.id
    total_files = len(file_paths)
    processed_files = 0
    results = []
    errors = []
    
    logger.info(f"[{worker_name}] Starting batch task {task_id}: {total_files} files in {folder_name}")
    
    # Начальный статус для batch задачи
    if task_id and not task_id.startswith('celery-'):
        update_task_status_sync(
            task_id,
            status="processing",
            progress=0,
            message=f"Начинается пакетная обработка: {total_files} файлов",
            folder_name=folder_name,
            total_files=total_files,
            started_at=datetime.now().isoformat()
        )
    
    try:
        for idx, file_path_str in enumerate(file_paths, 1):
            file_path = Path(file_path_str)
            filename = file_path.name
            
            try:
                progress = int((idx - 1) / total_files * 100)
                
                # Обновляем статус прогресса
                if task_id and not task_id.startswith('celery-'):
                    update_task_status_sync(
                        task_id,
                        progress=progress,
                        current_file=idx,
                        processed=processed_files,
                        errors=len(errors),
                        message=f'Обработка файла {idx}/{total_files}: {filename}'
                    )
                
                logger.info(f"[{worker_name}] [{idx}/{total_files}] Обработка: {filename}")
                
                # Запускаем задачу для отдельного файла
                async_result = generate_embedding.apply_async(
                    args=[str(file_path_str)],
                    kwargs={"onlyfile": True, "api_task_id": f"{task_id}_{idx}"},
                    queue="documents.tasks"
                )
                
                # Ждем завершения задачи
                task_result = async_result.get(timeout=300)
                
                if task_result.get('status') == 'skipped':
                    results.append({
                        'filename': filename,
                        'file_path': file_path_str,
                        'status': 'skipped',
                        'reason': task_result.get('reason')
                    })
                    logger.info(f"[{worker_name}] [{idx}/{total_files}] Пропущен: {filename}")
                else:
                    processed_files += 1
                    results.append({
                        'filename': filename,
                        'file_path': file_path_str,
                        'status': 'success',
                        'result': task_result
                    })
                    logger.info(f"[{worker_name}] [{idx}/{total_files}] Успешно: {filename}")
                    
            except Exception as e:
                error_msg = f"Ошибка обработки {filename}: {str(e)}"
                logger.error(f"[{worker_name}] {error_msg}")
                errors.append({
                    'filename': filename,
                    'file_path': file_path_str,
                    'error': error_msg
                })
        
        # Финальный результат batch обработки
        final_result = {
            "status": "completed",
            "folder_name": folder_name,
            "worker": worker_name,
            "total_files": total_files,
            "processed": processed_files,
            "errors_count": len(errors),
            "results": results,
            "errors": errors,
            "completed_at": datetime.now().isoformat()
        }
        
        logger.info(f"[{worker_name}] Пакетная обработка завершена: {processed_files}/{total_files} успешно")

        # Очистка папки если она была загружена
        if folder_name:
            uploads_dir = Path("uploads")
            folder_path = uploads_dir / folder_name

            if folder_path.exists() and folder_path.is_dir():
                shutil.rmtree(folder_path)
                logger.info(f"[{worker_name}] Папка {folder_path} удалена")

        # Финальное обновление статуса
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="completed",
                progress=100,
                message=f"Пакетная обработка завершена: {processed_files}/{total_files} успешно",
                result=final_result,
                completed_at=datetime.now().isoformat()
            )

        return final_result
        
    except Exception as e:
        error_msg = f"Критическая ошибка пакетной обработки: {str(e)}"
        logger.error(f"[{worker_name}] {error_msg}")
        
        if task_id and not task_id.startswith('celery-'):
            update_task_status_sync(
                task_id,
                status="failed",
                progress=100,
                message=error_msg,
                error={
                    "type": "batch_processing_error",
                    "message": error_msg
                },
                completed_at=datetime.now().isoformat()
            )
        
        raise
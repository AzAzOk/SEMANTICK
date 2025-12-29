import os
from celery import Celery
from celery.signals import worker_process_init
from pathlib import Path
from app.core.chanking import TextSplitter, DocumentChunker, BusinessMetadata
from app.core.parsers_system import ParserManager
from app.config import REDIS_URL
from app.database import init_qdrant, add_chunks_to_qdrant

# Создаем Celery приложение
celery_app = Celery(
    'semantic_search_app',
    broker=REDIS_URL,
    backend=REDIS_URL
)

# Настройки
celery_app.conf.update(
    result_expires=3600,
    task_track_started=True,
)

@worker_process_init.connect
def init_worker(**_):
    init_qdrant()

@celery_app.task(bind=True)
def generate_embedding(self, filename: str):
    """Обработка файла с отслеживанием прогресса"""
    
    worker_name = self.request.hostname
    
    try:
        print(f"[{worker_name}] Начинаю обработку файла: {filename}")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 1,
                'total_steps': 5,
                'progress': 0,
                'status': 'Чтение файла...',
                'filename': filename
            }
        )
        
        file_path = Path("uploads") / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"Файл {filename} не найден")
        
        print(f"[{worker_name}] Файл найден: {file_path.resolve()}")
        
        manager = ParserManager()
        splitter = TextSplitter()
        data_metadata = BusinessMetadata()
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 2,
                'total_steps': 5,
                'progress': 20,
                'status': 'Парсинг файла...',
                'filename': filename
            }
        )
        
        ext = manager._parser_extension(str(file_path))
        print(f"[{worker_name}] Расширение: {ext}")
        
        parser_class = manager._find_parser_in_registry(ext)
        if parser_class is None:
            raise ValueError(f"Парсер для .{ext} не найден")
        
        result_parser = manager._ransfer_selected_parser(str(file_path), parser_class)
        if result_parser is None:
            raise ValueError(f"Ошибка парсинга файла {filename}")
        
        text_length = len(result_parser.text)
        print(f"[{worker_name}] Парсинг завершен: {text_length} символов")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 3,
                'total_steps': 5,
                'progress': 40,
                'status': 'Разбиение на чанки...',
                'filename': filename
            }
        )
        
        chunks = splitter.split_text(result_parser.text)
        chunks_count = len(chunks)
        
        if chunks_count == 0:
            raise ValueError("Не удалось создать чанки")
        
        print(f"[{worker_name}] Создано чанков: {chunks_count}")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 4,
                'total_steps': 5,
                'progress': 60,
                'status': 'Создание метаданных...',
                'filename': filename
            }
        )
        
        metaDocument = DocumentChunker(chunks)
        result_uniter = metaDocument.uniter(
            result_parser.metadata,
            str(file_path),
            manager._file_name(str(file_path)),
            ext,
            data_metadata
        )
        print(f"[{worker_name}] Метаданные созданы")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 5,
                'total_steps': 5,
                'progress': 80,
                'status': 'Сохранение в Qdrant...',
                'filename': filename
            }
        )
        
        points = add_chunks_to_qdrant(result_uniter)
        print(f"[{worker_name}] Добавлено точек в Qdrant: {points}")
        
        file_path.unlink()
        print(f"[{worker_name}] Файл {filename} удален")
        
        result = {
            "status": "success",
            "filename": filename,
            "worker": worker_name,
            "text_length": text_length,
            "chunks_count": chunks_count,
            "points_added": points
        }
        
        print(f"[{worker_name}] Файл {filename} успешно обработан!")
        
        return result
        
    except FileNotFoundError as e:
        error_msg = str(e)
        print(f"[{worker_name}] Файл не найден: {error_msg}")
        
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'filename': filename,
                'status': 'Файл не найден'
            }
        )
        raise
        
    except ValueError as e:
        error_msg = str(e)
        print(f"[{worker_name}] Ошибка: {error_msg}")
        
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'filename': filename,
                'status': 'Ошибка обработки'
            }
        )
        raise
        
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        print(f"[{worker_name}] {error_msg}")
        
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'filename': filename,
                'status': 'Критическая ошибка'
            }
        )
        raise
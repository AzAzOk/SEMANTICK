import shutil
from celery import Celery
from celery.signals import worker_process_init
from pathlib import Path
from celery.exceptions import ImproperlyConfigured
from app.core.chanking import TextSplitter, DocumentChunker, BusinessMetadata
from app.core.parsers_system import ParserManager
from app.config import REDIS_URL
from app.database import init_qdrant, add_chunks_to_qdrant, reserch_file_name
SUPPORTED_EXTENSIONS = {'.txt','.pdf','.docx','.doc','.xlsx','.xls','.dxf','.dwg'}
import logging
logger = logging.getLogger(__name__)
celery_app = Celery(
    'semantic_search_app',
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery_app.conf.update(
    result_expires=3600,
    task_track_started=True,
    task_soft_time_limit=300,
    task_time_limit=360,
    
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    
    worker_max_tasks_per_child=50,
    
    broker_transport_options={
        'visibility_timeout': 3600,
        'socket_keepalive': True,
        'socket_keepalive_options': {
            1: 1,
            2: 1,
            3: 5,
        },
    },
)

@worker_process_init.connect
def init_worker(**_):
    init_qdrant()


def _cleanup_file(filename: str, worker_name: str = "") -> None:
    """Универсальная функция для удаления файла с логированием"""
    try:
        if filename.startswith("uploads") or filename.startswith("uploads\\"):
            file_path = Path(filename)
        else:
            file_path = Path("uploads") / filename
        
        if file_path.exists():
            file_path.unlink()
            logger.info(f"[{worker_name}] Файл удалён: {file_path}")
            print(f"[{worker_name}] Файл удалён: {file_path}")
            return True
        else:
            logger.warning(f"[{worker_name}] Файл не найден для удаления: {file_path}")
            return False
    except Exception as e:
        logger.error(f"[{worker_name}] Ошибка удаления файла {filename}: {e}")
        print(f"[{worker_name}] Ошибка удаления файла: {e}")
        return False


@celery_app.task(bind=True)
def generate_embedding(self, filename: str, onlyfile: bool = False):
    """Обработка файла с отслеживанием прогресса"""
    
    worker_name = self.request.hostname
    filenames = Path(filename)
    task_id = self.request.id
    # file_paths = None

    task_result = celery_app.AsyncResult(task_id)
    if task_result.state == 'REVOKED':
        print(f"[{worker_name}] Задача {task_id} отменена пользователем")
        _cleanup_file(filename, worker_name)
        return {
            "status": "cancelled",
            "message": "Задача отменена пользователем",
            "filename": filename
        }

    ext = filenames.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        _cleanup_file(filename, worker_name)
        return {
            "status": "skipped",
            "reason": "unsupported_format",
            "file_name": filename,
            "message": f"Файл {filename} пропущен (неподдерживаемый формат)"
        }

    try:
        dispach = filename
        if onlyfile:
            parts = filename.split('\\')
            dispach = '\\'.join(parts[2:]) if len(parts) > 2 else filename

        print(f"[{worker_name}] Проверка файла {dispach or filename} на дубликаты")

        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 1,
                'total_steps': 6,
                'progress': 0,
                'status': 'Сопоставление файла...',
                'filename': dispach or filename
            }
        )
        
        if reserch_file_name(dispach or filename):
            logger.info(f"[{worker_name}] Файл {dispach or filename} уже существует в базе данных")

            _cleanup_file(filename, worker_name)
            
            return {
                "status": "skipped",
                "reason": "already_exists",
                "file_name": dispach or filename,
                "message": f"Файл {dispach or filename} уже существует"
            }


        print(f"[{worker_name}] Начинаю обработку файла: {dispach or filename}")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 2,
                'total_steps': 6,
                'progress': 15,
                'status': 'Чтение файла...',
                'filename': dispach or filename
            }
        )
        
        if filename.startswith("uploads") or filename.startswith("uploads\\"):
            file_path = Path(filename)
        else:
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
                'current_step': 3,
                'total_steps': 6,
                'progress': 30,
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
        
        if len(result_parser.text) == 0:
            raise ValueError(f"Ошибка файл {filename} пустой")
        
        text_length = len(result_parser.text)
        print(f"[{worker_name}] Парсинг завершен: {text_length} символов")

        task_result = celery_app.AsyncResult(task_id)
        if task_result.state == 'REVOKED':
            _cleanup_file(filename, worker_name)
            return {"status": "cancelled"}
        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 4,
                'total_steps': 6,
                'progress': 45,
                'status': 'Разбиение на чанки...',
                'filename': filename
            }
        )
        
        chunks = splitter.split_text(result_parser.text)
        chunks_count = len(chunks)
        
        if chunks_count == 0:
            raise ValueError("Не удалось создать чанки")
            
        
        print(f"[{worker_name}] Создано чанков: {chunks_count}")

        task_result = celery_app.AsyncResult(task_id)
        if task_result.state == 'REVOKED':
            _cleanup_file(filename, worker_name)
            return {"status": "cancelled"}

        
        self.update_state(
            state='PROGRESS',
            meta={
                'current_step': 5,
                'total_steps': 6,
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
                'current_step': 6,
                'total_steps': 6,
                'progress': 80,
                'status': 'Сохранение в Qdrant...',
                'filename': filename
            }
        )
        logger.info(f"Колличество чанков на загрузку {len(result_uniter)}")
        points = add_chunks_to_qdrant(result_uniter)
        print(f"[{worker_name}] Добавлено точек в Qdrant: {points}")
        
        _cleanup_file(filename, worker_name)
        
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
        raise
        
    except ValueError as e:
        error_msg = str(e)
        print(f"[{worker_name}] Ошибка: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        print(f"[{worker_name}] {error_msg}")
        
        _cleanup_file(filename, worker_name)
        raise


@celery_app.task(bind=True)
def generate_embedding_batch(self, file_paths: list[str], folder_name: str = ""):
    """Обработка нескольких файлов из папки с общим отслеживанием прогресса
    
    Args:
        file_paths: Список относительных путей к файлам
        folder_name: Название папки для отображения в логах
    """
    
    worker_name = self.request.hostname
    total_files = len(file_paths)
    processed_files = 0
    results = []
    errors = []
    
    try:
        print(f"[{worker_name}] Начинаю пакетную обработку: {total_files} файлов")
        
        for idx, file_path_str in enumerate(file_paths, 1):
            file_path = Path(file_path_str)
            filename = file_path.name
            
            try:
                progress = int((idx - 1) / total_files * 100)
                
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current_file': idx,
                        'total_files': total_files,
                        'progress': progress,
                        'status': f'Обработка файла {idx}/{total_files}: {filename}',
                        'folder_name': folder_name,
                        'processed': processed_files,
                        'errors': len(errors)
                    }
                )
                
                print(f"[{worker_name}] [{idx}/{total_files}] Обработка: {file_path}")
                
                result = generate_embedding.apply(args=[str(file_path_str), True])
                print(f"Task ID: {result.id}")
                print(f"Status: {result.status}")
                if result.successful():
                    task_result = result.result
                    if isinstance(task_result, dict) and task_result.get('status') == 'skipped':
                        print(f"Пропущен (дубликат): {filename}")
                        results.append({
                            'filename': filename,
                            'file_path': file_path_str,
                            'result': task_result['status'],
                        })
                    else:
                        processed_files += 1
                        results.append({
                            'filename': filename,
                            'file_path': file_path_str,
                            'result': result.result,
                        })
                        print(f"[{worker_name}] [{idx}/{total_files}] Успешно: {filename}")
                else:
                    errors.append({
                        'filename': filename,
                        'file_path': file_path_str,
                        'error': str(result.result)
                    })
                    print(f"[{worker_name}] [{idx}/{total_files}] Ошибка: {filename}")
                    
            except Exception as e:
                error_msg = f"Ошибка обработки {filename}: {str(e)}"
                print(f"[{worker_name}] {error_msg}")
                errors.append({
                    'filename': filename,
                    'file_path': file_path_str,
                    'error': error_msg
                })
        
        final_result = {
            "status": "completed",
            "folder_name": folder_name,
            "worker": worker_name,
            "total_files": total_files,
            "processed": processed_files,
            "errors_count": len(errors),
            "results": results,
            "errors": errors
        }
        
        print(f"[{worker_name}] Пакетная обработка завершена: {processed_files}/{total_files} успешно")

        if folder_name:
            uploads_dir = Path("uploads")
            folder_path = uploads_dir / folder_name

            if folder_path.exists() and folder_path.is_dir():
                shutil.rmtree(folder_path)
                print(f"[{worker_name}] Папка {folder_path} удалена")

        self.update_state(
            state='SUCCESS',
            meta={
                'status': 'completed',
                'batch_results': results,
                'batch_errors': errors,
                'processed': processed_files,
                'total_files': total_files
            }
        )

        return final_result
        
    except Exception as e:
        error_msg = f"Критическая ошибка пакетной обработки: {str(e)}"
        print(f"[{worker_name}] {error_msg}")
        raise
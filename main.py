from app.core.chanking import TextSplitter, DocumentChunker, BusinessMetadata
from app.core.parsers_system import ParserManager
from app.database import client, add_chunks_to_qdrant, reserch_similar_chunks
import os
from pathlib import Path

def process_all_files_in_folder(folder_path):
    """
    Обрабатывает все файлы в указанной папке
    """
    path = Path(folder_path)
    
    # Проверяем существование папки
    if not path.exists():
        print(f"Папка {folder_path} не существует")
        return
    
    # Инициализируем менеджер один раз
    manager = ParserManager()
    splitter = TextSplitter()
    data_metadata = BusinessMetadata()
    
    # Проходим по всем файлам в папке
    for file_path in path.iterdir():
        # Пропускаем папки, обрабатываем только файлы
        if file_path.is_file():
            try:
                print(f"\n{'='*50}")
                print(f"Обрабатываю файл: {file_path.name}")
                print(f"{'='*50}")
                
                # Получаем расширение файла
                ext = manager._parser_extension(str(file_path))
                print(f"Расширение файла: {ext}")
                
                # Находим подходящий парсер
                find = manager._find_parser_in_registry(ext)
                print(f"Найден парсер: {find}")
                
                # Парсим файл
                result_parser = manager._ransfer_selected_parser(str(file_path), find)
                print(f"Парсинг завершен. Текст: {len(result_parser.text)} символов")
                
                # Разбиваем текст на чанки
                chunks = splitter.split_text(result_parser.text)
                print(f"Получено чанков: {len(chunks)}")
                
                # Создаем метаданные
                metaDocument = DocumentChunker(chunks)
                result_uniter = metaDocument.uniter(
                    result_parser.metadata, 
                    str(file_path), 
                    manager._file_name(str(file_path)), 
                    ext, 
                    data_metadata
                )
                print(f"Метаданные созданы")
                
                # Добавляем в Qdrant
                points = add_chunks_to_qdrant(client, result_uniter)
                print(f"Добавлено точек в Qdrant: {points}")
                
                print(f"✅ Файл {file_path.name} успешно обработан")
                
            except Exception as e:
                print(f"❌ Ошибка при обработке файла {file_path.name}: {str(e)}")
                continue

# Использование
if __name__ == "__main__":
    # Ваш путь к папке
    folder_path = "C:\\Users\\kulikovMA\\Desktop\\тест\\"
    
    # Инициализация клиента Qdrant (предполагается, что client уже создан)
    # client = QdrantClient(...)
    
    # Обрабатываем все файлы в папке
    process_all_files_in_folder(folder_path)
from .chunk_models import DocumentChunkData
from .chunk_models import ChunkMetadata, ChunkType
import uuid
from typing import Any, Dict, List


class DocumentChunker:

    """Создание объекта из чанка"""

    def __init__(self, text: list[str]):
        self.text = text
        self.unit_chunk: List[Dict[str, Any]] = []


    def uniter(self, metadata: Dict[str, Any],
                file_path: str,
                file_name: str,
                file_extension: str,
                business_metadata=None) -> List[Dict[str, Any]]:

        """
        Объединяет текстовые чанки с метаданными
        
        Args:
            metadata: Словарь с базовыми метаданными (file_path, file_name и т.д.)
            business_metadata: Опциональные бизнес-метаданные
            
        Returns:
            Список словарей с обработанными чанками
        """

        total_chunks = len(self.text)
        current_position = 0
        
        for i, text_chunk in enumerate(self.text):
            word_count = len(text_chunk.split())
            char_count = len(text_chunk)
            
            chunk_metadata = ChunkMetadata(
                file_path=file_path,
                file_name=file_name,
                file_extension=file_extension,
                chunk_index=i,
                total_chunks=total_chunks,
                start_position=current_position,
                end_position=current_position + char_count,
                parser_metadata=metadata,
                chunk_type=ChunkType.TEXT
            )
            
            chunk = DocumentChunkData(
                chunk_id=str(uuid.uuid4()),
                text=text_chunk,
                metadata=chunk_metadata,
                word_count=word_count,
                char_count=char_count,
                business_metadata=business_metadata
            )
            
            self.unit_chunk.append(chunk.to_dict())
            
            current_position += char_count + 1
        
        return self.unit_chunk
from typing import List, Optional
from .super_class import BaseParser, ParserResult
from loguru import logger

class PlainTextParser(BaseParser):

    """Парсер текстовых файлов"""
    
    def __init__(self, encodings: Optional[List[str]] = None):
        self.encodings = encodings or ['utf-8', 'cp1251', 'latin-1']
    
    def parse(self, file_path: str, **params) -> ParserResult:
        try:
            text = self._try_encodings(file_path)
            
            metadata = {
                'parser': 'PlainTextParser',
                'encoding': 'detected'
            }
            
            return ParserResult(
                success=True,
                text=text,
                error_message="",
                metadata=metadata,
                file_path=file_path
            )
        except Exception as e:
            logger.error(f"Text parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=str(e),
                metadata={},
                file_path=file_path
            )
    

    def _try_encodings(self, file_path: str) -> str:

        """Попытка прочитать с разными кодировками"""

        for encoding in self.encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
        
        # Последняя попытка с игнорированием ошибок
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    

    def get_supported_extensions(self) -> List[str]:
        return ['.txt', '.log', '.csv', '.md']
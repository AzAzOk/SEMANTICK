from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pathlib import Path
from dataclasses import dataclass

@dataclass
class ParserResult:

    """Результат парсинга документа"""

    success: bool
    text: str
    error_message: str
    metadata: Dict[str, Any]
    file_path: str
    
    def is_success(self) -> bool:
        return self.success
    
    def get_text(self) -> str:
        return self.text
    
    def get_error(self) -> str:
        return self.error_message
    
    def get_metadata(self) -> Dict:
        return self.metadata


class BaseParser(ABC):

    """Базовый интерфейс парсера"""
    
    @abstractmethod
    def parse(self, file_path: str, **params) -> ParserResult:

        """Парсинг файла"""

        pass
    

    @abstractmethod
    def get_supported_extensions(self) -> List[str]:

        """Получить поддерживаемые расширения"""

        pass
    

    def validate_file(self, file_path: str) -> bool:

        """Валидация файла"""

        if not Path(file_path).exists():
            return False
        
        ext = Path(file_path).suffix.lower()
        return ext in self.get_supported_extensions()
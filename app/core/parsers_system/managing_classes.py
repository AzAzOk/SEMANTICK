from typing import List, Dict, Any, Optional, Type
from pathlib import Path
import mimetypes
import pytesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
from .docx import DOCXParser
from .pdf import PDFParser
from .plain_text import PlainTextParser
from .image import ImageOCRParser
from .xlsx import XLSXParser
from .dxf import DXFParser
from .dwg import DWGParser
from .super_class import BaseParser


class FileValidator:

    """Валидатор файлов"""
    
    @staticmethod
    def validate_file_exists(file_path: str) -> bool:
        return Path(file_path).exists()
    

    @staticmethod
    def validate_file_size(file_path: str, max_size: int) -> bool:
        if not Path(file_path).exists():
            return False
        return Path(file_path).stat().st_size <= max_size
    

    @staticmethod
    def validate_file_type(file_path: str, expected_extensions: List[str]) -> bool:
        ext = Path(file_path).suffix.lower()
        return ext in expected_extensions
    

    @staticmethod
    def get_file_mime_type(file_path: str) -> str:
        mime_type, _ = mimetypes.guess_type(file_path)
        return mime_type or 'application/octet-stream'


class DepartmentConfig:

    """Конфигурация парсеров для отдела"""
    
    def __init__(self, department_name: str, max_file_size: int = 100 * 1024 * 1024, timeout: int = 300):
        self.department_name = department_name
        self.allowed_parsers: Dict[str, List[Type[BaseParser]]] = {}
        self.max_file_size = max_file_size
        self.timeout = timeout
        self._parser_priorities: Dict[str, Dict[Type[BaseParser], int]] = {}
    

    def get_allowed_extensions(self) -> List[str]:
        return list(self.allowed_parsers.keys())
    

    def is_parser_allowed(self, parser_class: Type[BaseParser]) -> bool:
        for parsers in self.allowed_parsers.values():
            if parser_class in parsers:
                return True
        return False
    

    def get_parser_priority(self, extension: str, parser_class: Type[BaseParser]) -> int:
        return self._parser_priorities.get(extension, {}).get(parser_class, 999)
    

    def register_parser(self, extension: str, parser_class: Type[BaseParser], priority: int = 10):
        if extension not in self.allowed_parsers:
            self.allowed_parsers[extension] = []
        
        if parser_class not in self.allowed_parsers[extension]:
            self.allowed_parsers[extension].append(parser_class)
        
        if extension not in self._parser_priorities:
            self._parser_priorities[extension] = {}
        self._parser_priorities[extension][parser_class] = priority


class ParserRegistry:

    """Реестр парсеров"""
    
    def __init__(self):
        self.global_registry: Dict[str, List[Type[BaseParser]]] = {}
        self.department_registries: Dict[str, DepartmentConfig] = {}
    

    def register_global_parser(self, extension: str, parser: Type[BaseParser]):
        if extension not in self.global_registry:
            self.global_registry[extension] = []
        if parser not in self.global_registry[extension]:
            self.global_registry[extension].append(parser)
    

    def register_department_parser(self, department: str, extension: str, parser: list[Type[BaseParser]]):
        if department not in self.department_registries:
            self.department_registries[department] = DepartmentConfig(department)
        
        self.department_registries[department].register_parser(extension, parser)
    

    def get_parsers_for_department(self, department: str, extension: str) -> List[Type[BaseParser]]:
        if department in self.department_registries:
            dept_config = self.department_registries[department]
            parsers = dept_config.allowed_parsers.get(extension, [])
            if parsers:
                return sorted(parsers, key=lambda p: dept_config.get_parser_priority(extension, p))
        
        return self.global_registry.get(extension, [])
    

    def get_all_supported_extensions(self) -> List[str]:
        extensions = set(self.global_registry.keys())
        for dept_config in self.department_registries.values():
            extensions.update(dept_config.get_allowed_extensions())
        return list(extensions)


class ParserManager:

    """Менеджер парсеров с поддержкой отделов"""
    
    def __init__(self):
        self.parser_registry = ParserRegistry()
        self.parser_instances: Dict[Type[BaseParser], BaseParser] = {}
        self.file_validator = FileValidator()
        
        self._init_default_parsers()
    

    def _init_default_parsers(self):

        """Инициализация стандартных парсеров"""

        default_parsers = {
            'pdf': PDFParser,
            'docx': DOCXParser,
            # 'doc': DOCParser,
            'xlsx': XLSXParser,
            # 'xls': XLSXParser,
            'txt': PlainTextParser,
            'dxf': DXFParser,
            # 'dwg': DWGParser,
            'png': ImageOCRParser,
            'jpg': ImageOCRParser,
            'jpeg': ImageOCRParser,
            'tiff': ImageOCRParser,
            'bmp': ImageOCRParser,
        }

        return default_parsers
    
            
    def _parser_extension(self, file_path: str) -> str:

        """Получение расширения файла"""

        return file_path.lower().split('.')[-1] if file_path else ''
    

    def _find_parser_in_registry(self, extension: str) -> Optional[Type[BaseParser]]:

        """Поиск подходящего парсера в реестре"""

        return self._init_default_parsers().get(extension)
    

    def _save_parser_instance(self, parser_class: Type[BaseParser]):

        """Сохранение экземпляра парсера"""

        if parser_class not in self.parser_instances:
            self.parser_instances[parser_class] = parser_class()

    def _ransfer_selected_parser(self, file_path: str, parser_class: Type[BaseParser]):

        """Передача файла выбранному парсеру"""

        if parser_class is None:
            return None
        parser_instance = parser_class()
        return parser_instance.parse(file_path)
    
    
    def _file_name(self, file_path: str) -> str:

        """Получение имени файла из пути"""

        return file_path.lower().split('\\')[-1] if file_path else None

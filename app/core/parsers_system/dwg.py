from typing import List, Dict, Any
import os
import ezdxf
from .super_class import BaseParser, ParserResult
from loguru import logger

class DWGParser(BaseParser):

    """Парсер DWG/DXF файлов (чертежи)"""
    
    def __init__(self, extract_metadata: bool = True, extract_geometry: bool = False):
        self.extract_metadata = extract_metadata
        self.extract_geometry = extract_geometry
    

    def parse(self, file_path: str, **params) -> ParserResult:
        try:
            doc = ezdxf.readfile(file_path)
            text = self._extract_text_entities(doc)
            
            metadata = self._parse_dwg_metadata(doc) if self.extract_metadata else {}
            metadata['parser'] = 'DWGParser'
            
            return ParserResult(
                success=True,
                text=text,
                error_message="",
                metadata=metadata,
                file_path=file_path
            )
        except Exception as e:
            logger.error(f"DWG parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=str(e),
                metadata={},
                file_path=file_path
            )
    

    def _parse_dwg_metadata(self, doc) -> Dict:
        """Извлечение метаданных DWG"""
        return {
            'dxf_version': doc.dxfversion,
            'layers_count': len(doc.layers)
        }
    

    def _extract_text_entities(self, doc) -> str:

        """Извлечение текстовых сущностей"""

        text = ""
        msp = doc.modelspace()
        
        for entity in msp:
            if entity.dxftype() == 'TEXT':
                text += entity.dxf.text + "\n"
            elif entity.dxftype() == 'MTEXT':
                text += entity.text + "\n"
        
        return text
    

    def get_supported_extensions(self) -> List[str]:
        return ['.dwg', '.dxf']
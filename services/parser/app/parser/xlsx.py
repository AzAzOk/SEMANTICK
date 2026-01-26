from typing import List, Optional
from .super_class import BaseParser, ParserResult
from loguru import logger

class XLSXParser(BaseParser):

    """Парсер Excel документов"""

    
    def __init__(self, read_formulas: bool = False, sheet_names: Optional[List[str]] = None):
        self.read_formulas = read_formulas
        self.sheet_names = sheet_names
    
    def parse(self, file_path: str, **params) -> ParserResult:
        try:
            import pandas as pd
            
            if self.sheet_names:
                excel_file = pd.read_excel(file_path, sheet_name=self.sheet_names)
            else:
                excel_file = pd.read_excel(file_path, sheet_name=None)
            
            text = self._parse_sheets(excel_file)
            
            metadata = {
                'parser': 'XLSXParser',
                'sheets_count': len(excel_file) if isinstance(excel_file, dict) else 1
            }
            
            return ParserResult(
                success=True,
                text=text,
                error_message="",
                metadata=metadata,
                file_path=file_path
            )
        except Exception as e:
            logger.error(f"XLSX parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=str(e),
                metadata={},
                file_path=file_path
            )
    

    def _parse_sheets(self, excel_file) -> str:

        """Парсинг листов Excel"""

        text = ""
        
        if isinstance(excel_file, dict):
            for sheet_name, df in excel_file.items():
                text += f"\n--- Лист: {sheet_name} ---\n"
                text += df.to_string() + "\n"
        else:
            text = excel_file.to_string()
        
        return text
    

    def get_supported_extensions(self) -> List[str]:
        return ['.xlsx']
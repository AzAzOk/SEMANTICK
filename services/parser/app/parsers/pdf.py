from typing import List, Dict, Any
import os
import pymupdf as fitz
from .super_class import BaseParser, ParserResult
from loguru import logger
import time
import io
import pymupdf as fitz
import pytesseract
from PIL import Image

class PDFParser(BaseParser):

    """Парсер PDF документов"""
    
    def __init__(self, use_ocr: bool = True, extract_images: bool = False, 
                 ocr_language: str = "rus+eng"):
        self.use_ocr = use_ocr
        self.extract_images = extract_images
        self.ocr_language = ocr_language
    

    def parse(self, file_path: str, **params) -> ParserResult:
        start_time = time.time()
        
        try:
            # Извлекаем параметры
            use_ocr = params.get('use_ocr', self.use_ocr)
            ocr_language = params.get('ocr_language', self.ocr_language)
            pages = params.get('pages', None)
            
            # Выбираем метод парсинга
            if use_ocr:
                text, metadata = self._extract_with_ocr(file_path, pages, ocr_language)
                method = "ocr"
            else:
                text, metadata = self._extract_with_pymupdf(file_path, pages)
                method = "text_extraction"
            
            # Извлекаем метаданные
            pdf_metadata = self._extract_pdf_metadata(file_path)
            
            # Формируем итоговые метаданные
            final_metadata = {
                'parser': 'PDFParser',
                'method': method,
                'use_ocr': use_ocr,
                'processing_time_sec': round(time.time() - start_time, 2),
                'text_length': len(text),
                **metadata,
                **pdf_metadata
            }
            
            return ParserResult(
                success=True,
                text=text,
                error_message="",
                metadata=final_metadata,
                file_path=file_path
            )
            
        except Exception as e:
            logger.error(f"PDF parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=str(e),
                metadata={},
                file_path=file_path
            )
    

    def _extract_with_pymupdf(self, file_path: str, pages: List[int] = None) -> tuple:

        """Извлечение текста с помощью PyMuPDF"""
        
        text = ""
        metadata = {}
        
        with fitz.open(file_path) as doc:
            metadata['total_pages'] = len(doc)
            metadata['is_encrypted'] = doc.is_encrypted
            
            # Определяем страницы для обработки
            if pages:
                page_indices = [p-1 for p in pages if 1 <= p <= len(doc)]
            else:
                page_indices = range(len(doc))
            
            for page_num in page_indices:
                page = doc[page_num]
                page_text = page.get_text()
                if page_text.strip():
                    text += f"--- Страница {page_num + 1} ---\n{page_text}\n"
            
            metadata['pages_processed'] = len(page_indices)
        
        return text, metadata
    

    def _extract_with_ocr(self, file_path: str, pages: List[int] = None, language: str = "rus+eng") -> tuple:

        """Извлечение текста с помощью OCR"""
        
        text = ""
        metadata = {
            'ocr_language': language,
            'ocr_engine': 'tesseract'
        }
        
        with fitz.open(file_path) as doc:
            metadata['total_pages'] = len(doc)
            
            # Определяем страницы для обработки
            if pages:
                page_indices = [p-1 for p in pages if 1 <= p <= len(doc)]
            else:
                page_indices = range(len(doc))
            
            ocr_pages_count = 0
            
            for page_num in page_indices:
                page = doc[page_num]
                
                # Сначала пробуем извлечь обычный текст
                page_text = page.get_text()
                if page_text.strip():
                    text += f"--- Страница {page_num + 1} (ТЕКСТ) ---\n{page_text}\n"
                else:
                    # Если текста нет - используем OCR
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # Увеличиваем разрешение
                    img_data = pix.tobytes("png")
                    
                    with Image.open(io.BytesIO(img_data)) as img:
                        ocr_text = pytesseract.image_to_string(img, lang=language)
                        if ocr_text.strip():
                            text += f"--- Страница {page_num + 1} (OCR) ---\n{ocr_text}\n"
                            ocr_pages_count += 1
            
            metadata['pages_processed'] = len(page_indices)
            metadata['ocr_pages'] = ocr_pages_count
        
        return text, metadata
    

    def _extract_pdf_metadata(self, file_path: str) -> Dict[str, Any]:

        """Извлечение метаданных PDF"""
        
        metadata = {}
        
        try:
            with fitz.open(file_path) as doc:
                pdf_metadata = doc.metadata
                metadata.update({
                    'author': pdf_metadata.get('author', ''),
                    'title': pdf_metadata.get('title', ''),
                    'subject': pdf_metadata.get('subject', ''),
                    'keywords': pdf_metadata.get('keywords', ''),
                    'creator': pdf_metadata.get('creator', ''),
                    'producer': pdf_metadata.get('producer', ''),
                    'creation_date': pdf_metadata.get('creationDate', ''),
                    'modification_date': pdf_metadata.get('modDate', ''),
                })
        except:
            pass  # Игнорируем ошибки метаданных
        
        # Добавляем информацию о файле
        file_stats = os.stat(file_path)
        metadata.update({
            'file_size_bytes': file_stats.st_size,
        })
        
        return metadata
    
    def get_supported_extensions(self) -> List[str]:
        return ['.pdf']
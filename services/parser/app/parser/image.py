from typing import List
from PIL import Image

from .super_class import BaseParser, ParserResult
from loguru import logger
import pytesseract

class ImageOCRParser(BaseParser):

    """Парсер изображений с OCR"""
    
    def __init__(self, ocr_language: str = 'rus+eng+ita+spa', image_quality: str = 'high'):
        self.ocr_language = ocr_language
        self.image_quality = image_quality
    

    def parse(self, file_path: str, **params) -> ParserResult:
        
        try:
            image = Image.open(file_path)
            preprocessed = self._preprocess_image(image)
            text = self._perform_ocr(preprocessed)
            
            metadata = {
                'parser': 'ImageOCRParser',
                'language': self.ocr_language,
                'image_size': image.size
            }
            
            return ParserResult(
                success=True,
                text=text,
                error_message="",
                metadata=metadata,
                file_path=file_path
            )
        except Exception as e:
            logger.error(f"OCR parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=str(e),
                metadata={},
                file_path=file_path
            )
    

    def _preprocess_image(self, image: Image.Image) -> Image.Image:

        """Предобработка изображения для OCR"""

        # Конвертация в grayscale
        return image.convert('L')
    

    def _perform_ocr(self, image: Image.Image) -> str:

        """Выполнение OCR"""

        return pytesseract.image_to_string(image, lang=self.ocr_language)
    

    def get_supported_extensions(self) -> List[str]:
        return ['.png', '.jpg', '.jpeg', '.tiff', '.bmp']
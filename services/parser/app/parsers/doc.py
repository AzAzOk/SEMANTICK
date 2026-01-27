from typing import List
import os
import tempfile
import subprocess
import shutil
from pathlib import Path
from .super_class import BaseParser, ParserResult
from .docx import DOCXParser
from loguru import logger

class DOCParser(BaseParser):
    """Парсер для старых MS Word `.doc` файлов."""

    def __init__(self):
        self.docx_parser = DOCXParser()
        self.has_antiword_python = self._check_antiword_python()

    def _check_antiword_python(self):
        """Проверяет наличие python-пакета antiword"""
        try:
            import antiword
            return True
        except ImportError:
            return False

    def parse(self, file_path: str, **params) -> ParserResult:
        temp_docx = None
        # temp_dir = tempfile.gettempdir()

        try:
            # ПОПЫТКА 1: Используем python-пакет antiword (если установлен)
            if self.has_antiword_python:
                try:
                    import antiword
                    logger.info(f"Используем python-пакет antiword для {file_path}")
                    
                    # Читаем файл как бинарный
                    with open(file_path, 'rb') as f:
                        content = f.read()
                    
                    # Используем antiword для парсинга
                    text = antiword.reader.read(content)
                    
                    if text and text.strip():
                        logger.success(f"Успешно распарсено с antiword: {len(text)} символов")
                        metadata = {
                            'parser': 'DOCParser→antiword(python)',
                            'original_format': 'DOC',
                            'text_length': len(text)
                        }
                        return ParserResult(
                            success=True, 
                            text=text, 
                            error_message="", 
                            metadata=metadata, 
                            file_path=file_path
                        )
                except Exception as e:
                    logger.warning(f"Python antiword не сработал: {e}")

            # # ПОПЫТКА 2: Используем бинарный antiword.exe (если есть)
            # antiword_exe = self._find_antiword_exe()
            # if antiword_exe:
            #     logger.info(f"Пробуем бинарный antiword: {antiword_exe}")
                
            #     try:
            #         # Получаем директорию с antiword.exe
            #         anti_dir = Path(antiword_exe).parent
                    
            #         # Пробуем разные кодировки
            #         encodings = ['UTF-8', 'cp1251', 'cp866', 'Default']
                    
            #         for encoding in encodings:
            #             try:
            #                 cmd = [antiword_exe]
            #                 if encoding != 'Default':
            #                     cmd.extend(['-m', encoding])
            #                 cmd.append(str(Path(file_path).resolve()))
                            
            #                 logger.debug(f"Запускаем команду: {' '.join(cmd)}")
                            
            #                 proc = subprocess.run(
            #                     cmd,
            #                     capture_output=True,
            #                     text=True,
            #                     encoding='utf-8',
            #                     errors='ignore',
            #                     timeout=30,
            #                     cwd=str(anti_dir),
            #                     shell=True
            #                 )
                            
            #                 if proc.returncode == 0 and proc.stdout and proc.stdout.strip():
            #                     text = proc.stdout
            #                     logger.success(f"Успешно с кодировкой {encoding}: {len(text)} символов")
                                
            #                     metadata = {
            #                         'parser': f'DOCParser→antiword(exe:{encoding})',
            #                         'original_format': 'DOC',
            #                         'text_length': len(text)
            #                     }
            #                     return ParserResult(
            #                         success=True, 
            #                         text=text, 
            #                         error_message="", 
            #                         metadata=metadata, 
            #                         file_path=file_path
            #                     )
            #                 else:
            #                     logger.debug(f"Кодировка {encoding} не сработала: {proc.stderr[:100] if proc.stderr else 'нет ошибки'}")
                                
            #             except Exception as e_enc:
            #                 logger.debug(f"Ошибка с кодировкой {encoding}: {e_enc}")
                            
            #     except Exception as e:
            #         logger.warning(f"Ошибка при запуске antiword.exe: {e}")

            # # ПОПЫТКА 3: LibreOffice (soffice) конвертация
            # soffice = self._find_soffice()
            # if soffice:
            #     logger.info(f"Пробуем LibreOffice: {soffice}")
                
            #     out_dir = os.path.join(temp_dir, f"doc_convert_out_{os.getpid()}")
            #     os.makedirs(out_dir, exist_ok=True)
                
            #     try:
            #         result = subprocess.run(
            #             [soffice, "--headless", "--convert-to", "docx", "--outdir", out_dir, file_path],
            #             capture_output=True,
            #             text=True,
            #             timeout=120,  # Увеличиваем таймаут
            #             shell=True
            #         )

            #         # Ищем созданный файл
            #         base_name = Path(file_path).stem
            #         candidate = os.path.join(out_dir, f"{base_name}.docx")
                    
            #         if not os.path.exists(candidate):
            #             # Может быть другое имя
            #             for f in os.listdir(out_dir):
            #                 if f.endswith('.docx'):
            #                     candidate = os.path.join(out_dir, f)
            #                     break
                    
            #         if os.path.exists(candidate):
            #             temp_docx = candidate
            #             logger.info(f"DOC конвертирован в DOCX: {temp_docx}")
                        
            #             try:
            #                 result = self.docx_parser.parse(temp_docx)
            #                 if result.success:
            #                     result.metadata['converted_from'] = 'doc'
            #                     result.metadata['parser_chain'] = 'DOCParser→DOCXParser'
            #                     result.file_path = file_path
            #                     logger.success(f"Успешная конвертация через LibreOffice")
            #                     return result
            #             except Exception as e_docx:
            #                 logger.warning(f"Ошибка при парсинге конвертированного DOCX: {e_docx}")
                            
            #     except Exception as e:
            #         logger.warning(f"Ошибка при вызове soffice: {e}")

            # # ПОПЫТКА 4: catdoc (если есть)
            # catdoc = shutil.which('catdoc') or self._find_executable('catdoc')
            # if catdoc:
            #     logger.info(f"Пробуем catdoc: {catdoc}")
                
            #     try:
            #         proc = subprocess.run(
            #             [catdoc, file_path],
            #             capture_output=True,
            #             text=True,
            #             encoding='utf-8',
            #             errors='ignore',
            #             timeout=30,
            #             shell=True
            #         )
                    
            #         if proc.returncode == 0 and proc.stdout and proc.stdout.strip():
            #             text = proc.stdout
            #             metadata = {
            #                 'parser': 'DOCParser→catdoc',
            #                 'original_format': 'DOC',
            #                 'text_length': len(text)
            #             }
            #             logger.success(f"Успешно с catdoc: {len(text)} символов")
            #             return ParserResult(
            #                 success=True, 
            #                 text=text, 
            #                 error_message="", 
            #                 metadata=metadata, 
            #                 file_path=file_path
            #             )
                        
            #     except Exception as e:
            #         logger.warning(f"Ошибка при запуске catdoc: {e}")

            # # ПОПЫТКА 5: win32com (только Windows)
            # try:
            #     text = self._parse_with_win32com(file_path)
            #     if text and text.strip():
            #         metadata = {
            #             'parser': 'DOCParser→win32com',
            #             'original_format': 'DOC',
            #             'text_length': len(text)
            #         }
            #         logger.success(f"Успешно с win32com: {len(text)} символов")
            #         return ParserResult(
            #             success=True, 
            #             text=text, 
            #             error_message="", 
            #             metadata=metadata, 
            #             file_path=file_path
            #         )
            # except Exception as e:
            #     logger.debug(f"win32com не доступен: {e}")

            # ВСЕ СПОСОБЫ НЕ СРАБОТАЛИ
            error_msg = (
                "Не удалось распарсить .doc файл. Установите один из:\n"
                "1. LibreOffice (для конвертации)\n"
                "2. antiword.exe (скачайте с официального сайта)\n"
                "3. pip install pywin32 для работы через COM"
            )
            logger.error(f"DOC parsing failed: {file_path}")
            
            return ParserResult(
                success=False, 
                text="", 
                error_message=error_msg, 
                metadata={
                    'parser': 'DOCParser',
                    'error': 'parsing_failed',
                    'original_format': 'DOC'
                }, 
                file_path=file_path
            )

        except Exception as e:
            logger.error(f"DOC parsing error: {e}")
            return ParserResult(
                success=False, 
                text="", 
                error_message=str(e), 
                metadata={'parser': 'DOCParser', 'error': str(e)}, 
                file_path=file_path
            )

        finally:
            # Удаляем временные файлы
            try:
                if temp_docx and os.path.exists(temp_docx):
                    os.unlink(temp_docx)
            except Exception:
                pass

    # def _find_antiword_exe(self) -> str:
    #     """Ищет бинарный файл antiword.exe"""
    #     # Проверяем PATH
    #     antiword = shutil.which('antiword')
    #     if antiword:
    #         return antiword

    #     # Проверяем стандартные пути
    #     candidates = r"C:\Program Files\antiword\antiword.exe"
        
    #     if os.path.exists(candidates):
    #         return candidates
        
    #     return None

    # def _parse_with_win32com(self, file_path: str) -> str:
    #     """Парсинг DOC через Microsoft Word COM API"""
    #     try:
    #         import win32com.client
    #         import pythoncom
            
    #         pythoncom.CoInitialize()
            
    #         word = win32com.client.Dispatch("Word.Application")
    #         word.visible = False
            
    #         doc = word.Documents.Open(os.path.abspath(file_path))
    #         text = doc.Content.Text
            
    #         doc.Close(False)
    #         word.Quit()
            
    #         pythoncom.CoUninitialize()
            
    #         return text
            
    #     except ImportError:
    #         raise Exception("pywin32 не установлен")
    #     except Exception as e:
    #         raise Exception(f"Ошибка COM: {e}")

    # def _find_soffice(self) -> str:
    #     # Проверяем PATH
    #     soffice = shutil.which('soffice')
    #     if soffice:
    #         return soffice

    #     # Популярные пути на Windows
    #     candidates = [
    #         r"C:\Program Files\LibreOffice\program\soffice.exe",
    #         r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
    #         r"C:\Program Files\OpenOffice\program\soffice.exe",
    #     ]
    #     for p in candidates:
    #         if os.path.exists(p):
    #             return p
    #     return None

    # def _find_executable(self, name: str) -> str:
    #     possible = [
    #         rf"C:\Program Files\{name}\{name}.exe",
    #         rf"C:\Program Files (x86)\{name}\{name}.exe",
    #     ]
    #     for p in possible:
    #         if os.path.exists(p):
    #             return p
    #     return None

    def get_supported_extensions(self) -> List[str]:
        return ['.doc']
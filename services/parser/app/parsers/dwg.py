from typing import List
import os
import tempfile
import subprocess
from pathlib import Path
from .super_class import BaseParser, ParserResult
from .dxf import DXFParser
from loguru import logger


class DWGParser(BaseParser):
    """
    Парсер DWG файлов через конвертацию в DXF.
    Конвертирует DWG → DXF → передаёт в DXFParser для извлечения текста.
    """
    
    def __init__(self):
        self.dxf_parser = DXFParser()
    
    def parse(self, file_path: str, **params) -> ParserResult:
        """
        Парсит DWG файл через конвертацию в DXF.
        
        Args:
            file_path: Путь к DWG файлу
            **params: Дополнительные параметры
            
        Returns:
            ParserResult с текстовым содержимым
        """
        temp_dxf_path = None
        
        try:
            # Конвертируем DWG в временный DXF файл
            temp_dxf_path = self._convert_dwg_to_dxf(file_path)
            
            if not temp_dxf_path or not os.path.exists(temp_dxf_path):
                raise FileNotFoundError("Не удалось создать временный DXF файл")
            
            # Парсим DXF с помощью DXFParser
            result = self.dxf_parser.parse(temp_dxf_path)
            
            # Обновляем метаданные: указываем, что это был DWG
            if result.success:
                result.metadata['original_format'] = 'DWG'
                result.metadata['converted_to'] = 'DXF'
                result.metadata['parser'] = 'DWGParser → DXFParser'
                result.file_path = file_path  # Возвращаем оригинальный путь
            
            return result
            
        except Exception as e:
            logger.error(f"DWG parsing error: {e}")
            return ParserResult(
                success=False,
                text="",
                error_message=f"DWG parsing error: {str(e)}",
                metadata={'parser': 'DWGParser', 'error': str(e)},
                file_path=file_path
            )
        
        finally:
            # Удаляем временные файлы и директории
            if temp_dxf_path and os.path.exists(temp_dxf_path):
                try:
                    os.unlink(temp_dxf_path)
                    # Удаляем временные директории, если они были созданы
                    temp_dir = tempfile.gettempdir()
                    for subdir in [f"dwg_input_{os.getpid()}", f"dwg_output_{os.getpid()}"]:
                        subdir_path = os.path.join(temp_dir, subdir)
                        if os.path.exists(subdir_path):
                            import shutil
                            shutil.rmtree(subdir_path)
                except Exception as cleanup_error:
                    logger.warning(f"Не удалось удалить временные файлы: {cleanup_error}")
    
    def _convert_dwg_to_dxf(self, dwg_path: str) -> str:
        """
        Конвертирует DWG в DXF через ODA File Converter или ezdxf.
        
        Args:
            dwg_path: Путь к DWG файлу
            
        Returns:
            Путь к временному DXF файлу
        """
        # Создаём временные директории
        temp_dir = tempfile.gettempdir()
        input_dir = os.path.join(temp_dir, f"dwg_input_{os.getpid()}")
        output_dir = os.path.join(temp_dir, f"dwg_output_{os.getpid()}")
        
        # Создаём директории
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # Копируем DWG файл во входную директорию
            input_dwg_path = os.path.join(input_dir, Path(dwg_path).name)
            import shutil
            shutil.copy2(dwg_path, input_dwg_path)
            
            # Ожидаемый путь выходного DXF
            output_dxf_path = os.path.join(output_dir, f"{Path(dwg_path).stem}.dxf")
            
            # Попытка 1: Используем ODA File Converter (если установлен)
            oda_converter_path = self._find_oda_converter()
            if oda_converter_path:
                try:
                    result = subprocess.run(
                        [oda_converter_path, input_dir, output_dir, "ACAD2018", "DXF", "0", "1"],
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    
                    if result.returncode == 0 and os.path.exists(output_dxf_path):
                        logger.info(f"DWG конвертирован в DXF через ODA File Converter: {output_dxf_path}")
                        return output_dxf_path
                    else:
                        logger.warning(f"ODA File Converter вернул код {result.returncode}")
                        if result.stderr:
                            logger.warning(f"ODA stderr: {result.stderr}")
                except Exception as oda_error:
                    logger.warning(f"ODA File Converter не сработал: {oda_error}")
            
        except Exception as copy_error:
            logger.warning(f"Ошибка при подготовке файлов для ODA: {copy_error}")
        
            # Попытка 2: Используем ezdxf (работает только с некоторыми версиями DWG)
            try:
                import ezdxf
                doc = ezdxf.readfile(dwg_path)
                doc.saveas(output_dxf_path)
                logger.info(f"DWG конвертирован в DXF через ezdxf: {output_dxf_path}")
                return output_dxf_path
            except Exception as ezdxf_error:
                logger.error(f"ezdxf не смог конвертировать DWG: {ezdxf_error}")
                raise RuntimeError(
                    f"Не удалось конвертировать DWG в DXF. "
                    f"Установите ODA File Converter или используйте совместимую версию DWG. "
                    f"Ошибка: {ezdxf_error}"
                )
        
        finally:
            # Очищаем временные директории
            try:
                import shutil
                if os.path.exists(input_dir):
                    shutil.rmtree(input_dir)
                # output_dir не удаляем - там результат
            except Exception as cleanup_error:
                logger.warning(f"Не удалось очистить временные файлы: {cleanup_error}")
    
    def _find_oda_converter(self) -> str:
        """
        Ищет ODA File Converter в системе.
        
        Returns:
            Путь к исполняемому файлу ODA File Converter или None
        """
        possible_paths = [
            r"C:\Program Files\ODA\ODAFileConverter\ODAFileConverter.exe",
            r"C:\Program Files (x86)\ODA\ODAFileConverter\ODAFileConverter.exe",
            r"C:\ODA\ODAFileConverter\ODAFileConverter.exe",
            r"C:\Program Files\ODA\ODAFileConverter 26.9.0\ODAFileConverter.exe",
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def get_supported_extensions(self) -> List[str]:
        """Поддерживаемые расширения файлов"""
        return ['.dwg']
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
    """Парсер для старых MS Word `.doc` файлов.

    Стратегия:
    1. Попытаться конвертировать `.doc` → `.docx` через LibreOffice (`soffice`).
    2. Если конвертация удалась — делегировать разбор `DOCXParser`.
    3. Фоллбек: попытаться получить plain-text через `antiword` или `catdoc`.
    4. Если ничего не доступно — выбросить информативную ошибку.
    """

    def __init__(self):
        self.docx_parser = DOCXParser()

    def parse(self, file_path: str, **params) -> ParserResult:
        temp_docx = None
        temp_dir = tempfile.gettempdir()

        try:
            # Попытка 1: LibreOffice (soffice) конвертация
            soffice = self._find_soffice()
            if soffice:
                out_dir = os.path.join(temp_dir, f"doc_convert_out_{os.getpid()}")
                os.makedirs(out_dir, exist_ok=True)
                try:
                    result = subprocess.run(
                        [soffice, "--headless", "--convert-to", "docx", "--outdir", out_dir, file_path],
                        capture_output=True,
                        text=True,
                        timeout=90,
                    )

                    # ожидаем файл с тем же именем, но .docx
                    candidate = os.path.join(out_dir, f"{Path(file_path).stem}.docx")
                    if os.path.exists(candidate):
                        temp_docx = candidate
                        logger.info(f"DOC конвертирован в DOCX через soffice: {temp_docx}")
                        result = self.docx_parser.parse(temp_docx)
                        # обновим метаданные
                        if result.success:
                            result.metadata.setdefault('converted_from', 'doc')
                            result.metadata.setdefault('parser_chain', 'DOCParser→DOCXParser')
                            result.file_path = file_path
                        return result
                    else:
                        logger.warning(f"soffice не создал файл {candidate}. stdout: {result.stdout} stderr: {result.stderr}")
                except Exception as e:
                    logger.warning(f"Ошибка при вызове soffice: {e}")

            # Попытка 2: antiword -> plain text
            antiword = shutil.which('antiword') or self._find_executable('antiword')
            if antiword:
                try:
                    anti_dir = Path(antiword).parent
                    # First try: run antiword from its install dir (so it can find mapping files)
                    proc = subprocess.run([antiword, str(file_path)], capture_output=True, text=True, timeout=30, cwd=str(anti_dir))
                    if proc.returncode == 0 and proc.stdout:
                        text = proc.stdout
                        metadata = {'parser': 'DOCParser→antiword', 'original_format': 'DOC'}
                        return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
                    else:
                        logger.warning(f"antiword failed (cwd={anti_dir}): rc={proc.returncode} stderr={proc.stderr}")

                    # Попробуем явно указать mapping-файл (передавая только имя mapping, запускаем в каталоге anti_dir)
                    try:
                        candidates = ['cp1251.txt', 'cp866.txt', 'UTF-8.txt', 'Default']
                        for cand in candidates:
                            mapping = anti_dir / cand
                            if mapping.exists():
                                proc2 = subprocess.run([antiword, '-m', cand, str(file_path)], capture_output=True, text=True, timeout=30, cwd=str(anti_dir))
                                if proc2.returncode == 0 and proc2.stdout:
                                    text = proc2.stdout
                                    metadata = {'parser': f'DOCParser→antiword (mapping={cand})', 'original_format': 'DOC'}
                                    return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
                                else:
                                    logger.warning(f"antiword with mapping {cand} failed: rc={proc2.returncode} stderr={proc2.stderr}")
                        # As a last resort, try invoking antiword via PowerShell like the user did
                        try:
                            # Try exact PowerShell invocation as the user suggested
                            antiword_path = str(Path(antiword))
                            mapping = 'cp1251.txt'
                            # Build PowerShell command exactly like: & 'C:\Program Files\antiword\antiword.exe' -m cp1251.txt 'C:\path\to\file.doc'
                            ps_cmd = f"& '{antiword_path}' -m {mapping} '{str(Path(file_path).resolve())}'"
                            proc3 = subprocess.run([
                                'powershell', '-NoProfile', '-Command', ps_cmd
                            ], capture_output=True, text=True, timeout=40)
                            if proc3.returncode == 0 and proc3.stdout:
                                text = proc3.stdout
                                metadata = {'parser': f"DOCParser→antiword (powershell mapping={mapping})", 'original_format': 'DOC'}
                                return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
                            else:
                                logger.warning(f"antiword via PowerShell mapping {mapping} failed: rc={proc3.returncode} stderr={proc3.stderr}")
                        except Exception as e_ps:
                            logger.warning(f"antiword via PowerShell attempt error: {e_ps}")
                    except Exception as e_map:
                        logger.warning(f"antiword mapping attempt error: {e_map}")

                except Exception as e:
                    logger.warning(f"antiword execution error: {e}")

            # Попытка 3: catdoc -> plain text
            catdoc = shutil.which('catdoc') or self._find_executable('catdoc')
            if catdoc:
                try:
                    proc = subprocess.run([catdoc, file_path], capture_output=True, text=True, timeout=30)
                    if proc.returncode == 0 and proc.stdout:
                        text = proc.stdout
                        metadata = {'parser': 'DOCParser→catdoc', 'original_format': 'DOC'}
                        return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
                    else:
                        logger.warning(f"catdoc failed: rc={proc.returncode} stderr={proc.stderr}")
                except Exception as e:
                    logger.warning(f"catdoc execution error: {e}")

            raise RuntimeError(
                "Не удалось конвертировать .doc: установите LibreOffice (soffice) или утилиты antiword/catdoc."
            )

        except Exception as e:
            logger.error(f"DOC parsing error: {e}")
            return ParserResult(success=False, text="", error_message=str(e), metadata={'parser': 'DOCParser', 'error': str(e)}, file_path=file_path)

        finally:
            # если создали временный docx — удалим
            try:
                if temp_docx and os.path.exists(temp_docx):
                    os.unlink(temp_docx)
            except Exception:
                pass

    def _find_soffice(self) -> str:
        # Проверяем PATH
        soffice = shutil.which('soffice')
        if soffice:
            return soffice

        # Популярные пути на Windows
        candidates = [
            r"C:\Program Files\LibreOffice\program\soffice.exe",
            r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
            r"C:\Program Files\OpenOffice\program\soffice.exe",
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def _find_executable(self, name: str) -> str:
        # Простая проверка обычных путей (Windows)
        possible = [
            rf"C:\Program Files\{name}\{name}.exe",
            rf"C:\Program Files (x86)\{name}\{name}.exe",
        ]
        for p in possible:
            if os.path.exists(p):
                return p
        return None

    def get_supported_extensions(self) -> List[str]:
        return ['.doc']

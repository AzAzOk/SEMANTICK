from typing import List
import os
from pathlib import Path
from loguru import logger

try:
	import xlrd  # type: ignore
except Exception:
	xlrd = None

try:
	import pandas as pd  # type: ignore
except Exception:
	pd = None

from .super_class import BaseParser, ParserResult


class XLSParser(BaseParser):
	"""Parser for old Excel `.xls` files.

	Strategy:
	- Try to use `xlrd` to read .xls workbooks.
	- If `xlrd` is not available, try `pandas.read_excel` as a fallback.
	- Extract text cell-by-cell, include sheet names and simple separators.
	"""

	def parse(self, file_path: str, **params) -> ParserResult:
		file_path = str(file_path)
		try:
			if xlrd:
				try:
					wb = xlrd.open_workbook(file_path, formatting_info=False)
					parts = []
					for sheet in wb.sheets():
						parts.append(f"== Sheet: {sheet.name} ==")
						for r in range(sheet.nrows):
							row = sheet.row_values(r)
							# join with tab, strip empty trailing
							row_text = "\t".join([str(cell) for cell in row if cell is not None and cell != ""])
							if row_text:
								parts.append(row_text)
					text = "\n".join(parts)
					metadata = {'parser': 'XLSParser', 'original_format': 'XLS'}
					return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
				except Exception as e_xlrd:
					logger.warning(f"xlrd failed to read {file_path}: {e_xlrd}")

			if pd:
				try:
					# read all sheets
					sheets = pd.read_excel(file_path, sheet_name=None, engine='xlrd' if xlrd else None)
					parts = []
					for sheet_name, df in sheets.items():
						parts.append(f"== Sheet: {sheet_name} ==")
						# convert DataFrame rows to tab-separated strings
						for _, row in df.fillna("").iterrows():
							row_text = "\t".join([str(x) for x in row.tolist() if x != ""])
							if row_text:
								parts.append(row_text)
					text = "\n".join(parts)
					metadata = {'parser': 'XLSParser→pandas', 'original_format': 'XLS'}
					return ParserResult(success=True, text=text, error_message="", metadata=metadata, file_path=file_path)
				except Exception as e_pd:
					logger.warning(f"pandas failed to read {file_path}: {e_pd}")

			raise RuntimeError("Не удалось прочитать .xls: установите библиотеку `xlrd` или `pandas` с поддержкой чтения .xls")

		except Exception as e:
			logger.error(f"XLS parsing error for {file_path}: {e}")
			return ParserResult(success=False, text="", error_message=str(e), metadata={'parser': 'XLSParser', 'error': str(e)}, file_path=file_path)

	def get_supported_extensions(self) -> List[str]:
		return ['.xls']


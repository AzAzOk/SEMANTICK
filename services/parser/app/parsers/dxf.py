from typing import List, Dict, Any
import ezdxf
import re
import hashlib
from collections import defaultdict
from .super_class import BaseParser, ParserResult


class DXFParser(BaseParser):
    """Парсер DXF файлов (AutoCAD Drawing Exchange Format)"""
    
    def __init__(self):
        self.text_hash_map = {}
        self.extracted_texts = []
    
    def parse(self, file_path: str, **params) -> ParserResult:
        """
        Парсинг DXF файла с извлечением всего текстового содержимого
        
        Args:
            file_path: Путь к DXF файлу
            **params: Дополнительные параметры (игнорируются)
            
        Returns:
            ParserResult с текстовым содержимым
        """
        try:
            # Открываем DXF файл
            dxf_doc = ezdxf.readfile(file_path)
            
            # Сбрасываем состояние
            self.text_hash_map.clear()
            self.extracted_texts.clear()
            
            # Обработка modelspace
            msp = dxf_doc.modelspace()
            self._process_entities(msp, context='modelspace')
            
            # Обработка всех layouts (включая Paper_Space)
            for layout in dxf_doc.layouts:
                self._process_entities(layout, context=f'layout_{layout.name}')
            
            # Обработка блоков
            for block in dxf_doc.blocks:
                if not block.name.startswith('*'):  # Пропускаем системные блоки
                    self._process_entities(block, context=f'block_{block.name}')
            
            # Дедупликация текстов
            deduplicated_data = self._deduplicate_texts(self.extracted_texts)
            
            # Формирование итогового текста
            final_text = self._format_text_output(deduplicated_data)
            
            # Минимальные метаданные
            metadata = {
                'dxf_version': str(dxf_doc.dxfversion),
                'total_texts': len(deduplicated_data),
                'unique_texts': len(deduplicated_data),
                'text_types': self._count_text_types(deduplicated_data)
            }
            
            return ParserResult(
                success=True,
                text=final_text,
                error_message="",
                metadata=metadata,
                file_path=file_path
            )
            
        except Exception as e:
            return ParserResult(
                success=False,
                text="",
                error_message=f"DXF parsing error: {str(e)}",
                metadata={},
                file_path=file_path
            )
    
    def _process_entities(self, container, context='main'):
        """Универсальный обход всех сущностей в контейнере"""
        for entity in container:
            self._process_entity(entity, context)
    
    def _process_entity(self, entity, context):
        """Диспетчеризация по типу сущности"""
        entity_type = entity.dxftype()
        handler = self._entity_handlers().get(entity_type)
        
        if handler:
            result = handler(entity, context)
            if result is not None:
                if isinstance(result, list):
                    for item in result:
                        item['original_entity'] = entity
                        self.extracted_texts.append(item)
                else:
                    result['original_entity'] = entity
                    self.extracted_texts.append(result)
    
    def _entity_handlers(self):
        """Словарь обработчиков по типу сущности"""
        return {
            'TEXT': self._extract_text_data,
            'MTEXT': self._extract_mtext_data,
            'ATTDEF': self._extract_attdef_data,
            'ATTRIB': self._extract_attrib_data,
            'INSERT': self._extract_insert_data,
            'ACAD_TABLE': self._extract_acad_table_data
        }
    
    def _extract_text_data(self, entity, context):
        """Извлечение TEXT"""
        text = entity.dxf.text
        if not text or not text.strip():
            return None
            
        return {
            'type': 'TEXT',
            'text': text,
            'plain_text': self._extract_plain_text(text),
            'context': context,
            'position': getattr(entity.dxf, 'insert', None)
        }
    
    def _extract_mtext_data(self, entity, context):
        """Извлечение MTEXT"""
        text = entity.text
        if not text or not text.strip():
            return None
            
        return {
            'type': 'MTEXT',
            'text': text,
            'plain_text': self._extract_plain_text(text),
            'context': context,
            'position': getattr(entity, 'insert', None)
        }
    
    def _extract_attdef_data(self, entity, context):
        """Извлечение ATTDEF"""
        text = entity.dxf.text
        if not text or not text.strip():
            return None
            
        tag = getattr(entity.dxf, 'tag', '')
        return {
            'type': 'ATTDEF',
            'text': text,
            'plain_text': self._extract_plain_text(text),
            'tag': tag,
            'context': context,
            'position': getattr(entity.dxf, 'insert', None)
        }
    
    def _extract_attrib_data(self, entity, context):
        """Извлечение ATTRIB"""
        text = entity.dxf.text
        if not text or not text.strip():
            return None
            
        tag = getattr(entity.dxf, 'tag', '')
        return {
            'type': 'ATTRIB',
            'text': text,
            'plain_text': self._extract_plain_text(text),
            'tag': tag,
            'context': context,
            'position': getattr(entity.dxf, 'insert', None)
        }
    
    def _extract_insert_data(self, entity, context):
        """Извлечение атрибутов из INSERT (блоков)"""
        attribs = getattr(entity, 'attribs', [])
        attrib_texts = []
        
        for attrib in attribs:
            attrib_data = self._extract_attrib_data(
                attrib, 
                context=f'insert_{getattr(entity.dxf, "name", "unknown")}'
            )
            if attrib_data:
                attrib_texts.append(attrib_data)
        
        return attrib_texts if attrib_texts else None
    
    def _extract_acad_table_data(self, entity, context):
        """
        КРИТИЧЕСКИ ВАЖНО: Извлечение текста из ACAD_TABLE с сохранением структуры
        """
        table_texts = []
        
        try:
            # Получаем proxy-содержимое таблицы
            proxy_content = list(entity.proxy_graphic_content())
            
            # Собираем все текстовые сущности
            text_entities = []
            for i, proxy_entity in enumerate(proxy_content):
                entity_type = proxy_entity.dxftype()
                
                if entity_type in ['TEXT', 'MTEXT']:
                    # Определяем текст в зависимости от типа
                    if entity_type == 'TEXT':
                        entity_text = proxy_entity.dxf.text
                        position = getattr(proxy_entity.dxf, 'insert', None)
                    else:  # MTEXT
                        entity_text = proxy_entity.text
                        position = getattr(proxy_entity, 'insert', None)
                    
                    if not entity_text or not entity_text.strip():
                        continue
                    
                    text_entities.append({
                        'index': i,
                        'text': entity_text,
                        'position': position,
                        'entity_type': entity_type
                    })
            
            # Сортируем по позиции (Y убывает = row растёт, X возрастает = col растёт)
            text_entities.sort(key=lambda x: (
                -x['position'][1] if x['position'] else 0,
                x['position'][0] if x['position'] else 0
            ))
            
            # Группируем по строкам (Y-координата)
            y_groups = {}
            y_tolerance = 5.0
            
            for te in text_entities:
                if te['position']:
                    y = te['position'][1]
                    # Ищем существующую группу с близкой Y-координатой
                    found_group = None
                    for existing_y in y_groups.keys():
                        if abs(y - existing_y) <= y_tolerance:
                            found_group = existing_y
                            break
                    
                    if found_group is not None:
                        y_groups[found_group].append(te)
                    else:
                        y_groups[y] = [te]
            
            # Определяем количество колонок
            max_cols = max(len(row_items) for row_items in y_groups.values()) if y_groups else 1
            
            # Формируем строки таблицы
            sorted_rows = sorted(y_groups.keys(), reverse=True)
            table_rows = []
            
            for row_idx, y_coord in enumerate(sorted_rows):
                row_items = sorted(y_groups[y_coord], key=lambda x: x['position'][0] if x['position'] else 0)
                row_texts = [self._extract_plain_text(item['text']) for item in row_items]
                table_rows.append(' | '.join(row_texts))
            
            # Создаём запись для таблицы
            if table_rows:
                table_text = '\n'.join(table_rows)
                table_texts.append({
                    'type': 'ACAD_TABLE',
                    'text': table_text,
                    'plain_text': table_text,
                    'context': f"{context}:table",
                    'rows_count': len(table_rows),
                    'cols_count': max_cols
                })
            
        except Exception as e:
            # В случае ошибки просто пропускаем таблицу
            pass
        
        return table_texts if table_texts else None
    
    def _extract_plain_text(self, text: str) -> str:
        """Очистка текста от DXF форматирования"""
        if not text:
            return text
        
        # Нормализация управляющих последовательностей
        clean = text.replace('\\P', '\n').replace('\\p', '\n').replace('\\n', '\n')
        
        # Обработка групп вида {\C1;Some text}
        clean = re.sub(r'\{\\[A-Za-z0-9]+;([^}]*)\}', r'\1', clean)
        
        # Удаляем escape-последовательности
        clean = re.sub(r'\\[A-Za-z]+[0-9\.\-]*;?', '', clean)
        
        # Убираем фигурные скобки
        clean = clean.replace('{', '').replace('}', '')
        
        # Сжимаем множественные пробелы
        clean = re.sub(r'[ \t]+', ' ', clean).strip()
        
        return clean
    
    def _deduplicate_texts(self, text_data: List[Dict]) -> List[Dict]:
        """Дедупликация текстов по MD5 хешу"""
        text_groups = defaultdict(list)
        unique_data = []
        
        for item in text_data:
            full_text_content = item["text"].strip()
            text_hash = hashlib.md5(full_text_content.encode('utf-8')).hexdigest()
            text_groups[text_hash].append(item)
        
        for text_hash, items in text_groups.items():
            if not items:
                continue
            
            # Берём первый элемент как представителя группы
            representative = items[0].copy()
            representative["duplicate_count"] = len(items)
            
            unique_data.append(representative)
        
        return unique_data
    
    def _format_text_output(self, text_data: List[Dict]) -> str:
        """Форматирование итогового текстового вывода"""
        output_lines = []
        
        # Группируем по контексту
        by_context = defaultdict(list)
        for item in text_data:
            by_context[item.get('context', 'unknown')].append(item)
        
        # Выводим тексты по контекстам
        for context in sorted(by_context.keys()):
            items = by_context[context]
            
            # Заголовок секции
            output_lines.append(f"\n=== {context.upper()} ===\n")
            
            for item in items:
                plain_text = item.get('plain_text', '').strip()
                if plain_text:
                    text_type = item.get('type', 'UNKNOWN')
                    
                    # Для таблиц добавляем информацию о структуре
                    if text_type == 'ACAD_TABLE':
                        rows = item.get('rows_count', 0)
                        cols = item.get('cols_count', 0)
                        output_lines.append(f"[TABLE {rows}x{cols}]")
                        output_lines.append(plain_text)
                        output_lines.append("")
                    # Для атрибутов показываем тег
                    elif 'tag' in item and item['tag']:
                        output_lines.append(f"[{item['tag']}]: {plain_text}")
                    # Для обычного текста
                    else:
                        output_lines.append(plain_text)
        
        return '\n'.join(output_lines)
    
    def _count_text_types(self, text_data: List[Dict]) -> Dict[str, int]:
        """Подсчёт количества текстов по типам"""
        counts = defaultdict(int)
        for item in text_data:
            counts[item.get('type', 'UNKNOWN')] += 1
        return dict(counts)
    
    def get_supported_extensions(self) -> List[str]:
        """Поддерживаемые расширения файлов"""
        return ['.dxf']
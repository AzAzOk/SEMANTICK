from typing import List, Dict, Any
import ezdxf
from .super_class import BaseParser, ParserResult

class DXFParser(BaseParser):

    """Парсер DXF файлов (AutoCAD Drawing Exchange Format)"""
    
    def __init__(self, extract_metadata: bool = True, extract_blocks: bool = True,
                 search_in_blocks: bool = True, search_in_layouts: bool = True):
        self.extract_metadata = extract_metadata
        self.extract_blocks = extract_blocks
        self.search_in_blocks = search_in_blocks
        self.search_in_layouts = search_in_layouts
    

    def parse(self, file_path: str, **params) -> ParserResult:
        try:
            # Извлекаем параметры поиска
            search_in_blocks = params.get('search_in_blocks', self.search_in_blocks)
            search_in_layouts = params.get('search_in_layouts', self.search_in_layouts)
            search_in_entities = params.get('search_in_entities', True)
            deep_search = params.get('deep_search', True)
            
            # Открываем DXF файл
            doc = ezdxf.readfile(file_path)
            
            # Ищем текст в РАЗНЫХ местах
            text_content = ""
            
            if search_in_entities:
                text_content += self._extract_from_entities(doc)
            
            if search_in_blocks:
                text_content += self._extract_from_blocks(doc, deep_search)
            
            if search_in_layouts:
                text_content += self._extract_from_layouts(doc)
            
            # Извлекаем метаданные
            metadata = self._extract_metadata(doc) if self.extract_metadata else {}
            
            # Формируем итоговый текст
            final_text = self._format_output(text_content, metadata, params)
            
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
    
    def _extract_from_entities(self, doc) -> str:

        """Извлечение текста из entities (прямых объектов)"""

        text_parts = []
        msp = doc.modelspace()
        
        # Счетчики найденных объектов
        counters = {'TEXT': 0, 'MTEXT': 0, 'ATTDEF': 0, 'ATTRIB': 0}
        
        # TEXT entities
        for text in msp.query('TEXT'):
            if text.dxf.text and text.dxf.text.strip():
                text_parts.append(f"TEXT: {text.dxf.text}")
                counters['TEXT'] += 1
        
        # MTEXT entities (многострочный текст)
        for mtext in msp.query('MTEXT'):
            if mtext.text and mtext.text.strip():
                text_parts.append(f"MTEXT: {mtext.text}")
                counters['MTEXT'] += 1
        
        # ATTDEF (определения атрибутов)
        for attdef in msp.query('ATTDEF'):
            if attdef.dxf.tag and attdef.dxf.default_value:
                text_parts.append(f"ATTR_DEF: {attdef.dxf.tag} = {attdef.dxf.default_value}")
                counters['ATTDEF'] += 1
        
        # ATTRIB (атрибуты вставленных блоков)
        for attrib in msp.query('ATTRIB'):
            if attrib.dxf.text and attrib.dxf.text.strip():
                text_parts.append(f"ATTR: {attrib.dxf.text}")
                counters['ATTRIB'] += 1
        
        # Добавляем статистику
        if any(counters.values()):
            text_parts.append(f"\n📊 Найдено в entities: TEXT={counters['TEXT']}, MTEXT={counters['MTEXT']}, ATTDEF={counters['ATTDEF']}, ATTRIB={counters['ATTRIB']}")
        
        return "\n".join(text_parts) + "\n" if text_parts else ""
    

    def _extract_from_blocks(self, doc, deep_search: bool = True) -> str:

        """Извлечение текста из блоков"""

        text_parts = []
        total_blocks_searched = 0
        total_text_found = 0
        
        for block in doc.blocks:
            # Пропускаем системные блоки
            if block.name.startswith('*'):
                continue
                
            total_blocks_searched += 1
            block_text_parts = []
            
            # Ищем текст внутри блока
            for entity in block:
                entity_text = self._extract_text_from_entity(entity)
                if entity_text:
                    block_text_parts.append(f"  - {entity_text}")
                    total_text_found += 1
            
            # Если в блоке нашли текст - добавляем в результат
            if block_text_parts:
                text_parts.append(f"🔷 БЛОК: {block.name}")
                text_parts.extend(block_text_parts)
                text_parts.append("")  # Пустая строка для разделения
        
        # Добавляем статистику по блокам
        if total_blocks_searched > 0:
            text_parts.append(f"📊 Поиск в блоках: проверено {total_blocks_searched} блоков, найдено текста в {total_text_found} местах")
        
        return "\n".join(text_parts) + "\n" if text_parts else ""
    

    def _extract_from_layouts(self, doc) -> str:

        """Извлечение текста из layout'ов (Paper Space)"""

        text_parts = []
        
        for layout in doc.layouts:
            # Пропускаем Model Space (уже обработали)
            if layout.name == 'Model':
                continue
                
            layout_text_parts = []
            
            # Ищем текст в layout'е
            for entity in layout:
                entity_text = self._extract_text_from_entity(entity)
                if entity_text:
                    layout_text_parts.append(f"  - {entity_text}")
            
            # Если в layout'е нашли текст
            if layout_text_parts:
                text_parts.append(f"📄 LAYOUT: {layout.name}")
                text_parts.extend(layout_text_parts)
                text_parts.append("")
        
        return "\n".join(text_parts) + "\n" if text_parts else ""
    

    def _extract_text_from_entity(self, entity) -> str:

        """Извлечение текста из конкретного entity"""

        try:
            if entity.dxftype() == 'TEXT' and entity.dxf.text and entity.dxf.text.strip():
                return f"TEXT: {entity.dxf.text}"
            elif entity.dxftype() == 'MTEXT' and entity.text and entity.text.strip():
                return f"MTEXT: {entity.text}"
            elif entity.dxftype() == 'ATTDEF' and entity.dxf.tag and entity.dxf.default_value:
                return f"ATTR_DEF: {entity.dxf.tag} = {entity.dxf.default_value}"
            elif entity.dxftype() == 'ATTRIB' and entity.dxf.text and entity.dxf.text.strip():
                return f"ATTR: {entity.dxf.text}"
        except:
            pass
        return ""
    

    def _extract_metadata(self, doc) -> Dict[str, Any]:

        """Извлечение метаданных DXF файла"""

        metadata = {
            'dxf_version': str(doc.dxfversion),
            'layers_count': len(doc.layers),
            'blocks_count': len(doc.blocks),
            'entities_count': len(doc.modelspace()),
            'layouts_count': len(doc.layouts) - 1,  # -1 потому что Model тоже layout
            'file_units': str(doc.header.get('$INSUNITS', 'Unknown')),
        }
        
        # Статистика по типам объектов
        msp = doc.modelspace()
        entity_stats = {
            'TEXT': len(msp.query('TEXT')),
            'MTEXT': len(msp.query('MTEXT')),
            'ATTDEF': len(msp.query('ATTDEF')),
            'ATTRIB': len(msp.query('ATTRIB')),
            'INSERT': len(msp.query('INSERT')),  # Вставки блоков
        }
        metadata['entity_statistics'] = entity_stats
        
        # Информация о слоях
        layers_info = []
        for layer in doc.layers:
            layers_info.append({
                'name': layer.dxf.name,
                'color': layer.dxf.color,
                'is_off': layer.is_off(),
            })
        metadata['layers'] = layers_info
        
        # Информация о блоках
        blocks_info = []
        for block in doc.blocks:
            if not block.name.startswith('*'):
                blocks_info.append({
                    'name': block.name,
                    'entities_count': len(block),
                })
        metadata['blocks'] = blocks_info
        
        return metadata
    

    def _format_output(self, text_content: str, metadata: Dict, params: Dict) -> str:

        """Форматирование итогового текста"""

        output_parts = []
        
        # Добавляем метаданные
        if params.get('include_metadata', True):
            output_parts.append("=== МЕТАДАННЫЕ DXF ===")
            output_parts.append(f"Версия DXF: {metadata.get('dxf_version', 'Unknown')}")
            output_parts.append(f"Количество слоев: {metadata.get('layers_count', 0)}")
            output_parts.append(f"Количество блоков: {metadata.get('blocks_count', 0)}")
            output_parts.append(f"Количество объектов в Model: {metadata.get('entities_count', 0)}")
            
            # Статистика объектов
            stats = metadata.get('entity_statistics', {})
            output_parts.append(f"📊 Объекты: TEXT={stats.get('TEXT', 0)}, MTEXT={stats.get('MTEXT', 0)}, ATTDEF={stats.get('ATTDEF', 0)}, ATTRIB={stats.get('ATTRIB', 0)}")
            output_parts.append("")
        
        # Добавляем текстовое содержимое
        if text_content.strip():
            output_parts.append("=== ТЕКСТОВОЕ СОДЕРЖИМОЕ ===")
            output_parts.append(text_content)
        else:
            output_parts.append("=== ТЕКСТ НЕ НАЙДЕН ===")
            output_parts.append("Текстовые объекты не найдены в:")
            output_parts.append("- Model Space entities")
            output_parts.append("- Блоках (blocks)")
            output_parts.append("- Layout'ах (Paper Space)")
            output_parts.append("")
            output_parts.append("💡 Возможные причины:")
            output_parts.append("- Текст находится во вставленных блоках (INSERT)")
            output_parts.append("- Файл содержит только геометрию без текста")
            output_parts.append("- Текст в специализированных объектах")
        
        return "\n".join(output_parts)
    

    def get_supported_extensions(self) -> List[str]:
        return ['.dxf']
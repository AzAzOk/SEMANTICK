from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from enum import Enum
import uuid


class ChunkType(str, Enum):

    """Типы чанков"""

    TEXT = "text"


@dataclass
class ChunkMetadata:

    """тех метаданные чанка"""

    file_path: str
    file_name: str
    file_extension: str
    chunk_index: int
    total_chunks: int
    start_position: int
    end_position: int
    parser_metadata: Dict[str, Any]
    chunk_type: ChunkType = ChunkType.TEXT

@dataclass  
class BusinessMetadata:

    """Метаданные из БД"""

    doc_number: str
    file_path: str
    
    id: int = 1

    customer_name: Optional[str] = None
    customer_status: Optional[int] = None

    enterprise_name: Optional[str] = None
    enterprise_status: Optional[int] = None

    stage_name: Optional[str] = None
    stage_status: Optional[int] = None

    title_number_name: Optional[str] = None
    title_number_name_status: Optional[int] = None

    title_name: Optional[str] = None
    title_status: Optional[int] = None

    discipline_code_name: Optional[str] = None
    discipline_code_status: Optional[int] = None

    mark_name: Optional[str] = None
    mark_status: Optional[int] = None

    title: Optional[str] = None
    title_status: Optional[int] = None

    version_name: Optional[str] = None
    version_status: Optional[int] = None

    language_name: Optional[str] = None
    language_status: Optional[int] = None
    
    gip_name: Optional[str] = None
    gip_status: Optional[int] = None

    developer_name: Optional[str] = None
    developer_status: Optional[int] = None

    region_name: Optional[str] = None
    region_status: Optional[int] = None

    status_name: Optional[str] = None
    status_status: Optional[int] = None

@dataclass
class DocumentChunkData:

    chunk_id: str
    text: str
    metadata: ChunkMetadata
    word_count: int
    char_count: int
    business_metadata: Optional[BusinessMetadata] = None

    def to_dict(self, include_business: bool = True) -> Dict[str, Any]:
        
        """Преобразует чанк в словарь для векторной БД"""
        
        result = {
            'chunk_id': self.chunk_id,
            'text': self.text,
            'word_count': self.word_count,
            'char_count': self.char_count,
            'metadata': {
                'file_path': self.metadata.file_path,
                'file_name': self.metadata.file_name,
                'file_extension': self.metadata.file_extension,
                'chunk_index': self.metadata.chunk_index,
                'total_chunks': self.metadata.total_chunks,
                'chunk_type': self.metadata.chunk_type.value,
                'start_position': self.metadata.start_position,
                'end_position': self.metadata.end_position,
                'parser_metadata': self.metadata.parser_metadata,
            }
        }
        
        if include_business and self.business_metadata:
            result['business_metadata'] = {
                'doc_number': self.business_metadata.doc_number,
                'file_path': self.business_metadata.file_path,
                
                'customer_name': self.business_metadata.customer_name,
                'customer_status': self.business_metadata.customer_status,
                'enterprise_name': self.business_metadata.enterprise_name,
                'enterprise_status': self.business_metadata.enterprise_status,
                'stage_name': self.business_metadata.stage_name,
                'stage_status': self.business_metadata.stage_status,
                'title_number_name': self.business_metadata.title_number_name,
                'title_number_name_status': self.business_metadata.title_number_name_status,
                'title_name': self.business_metadata.title_name,
                'title_status': self.business_metadata.title_status,
                'discipline_code_name': self.business_metadata.discipline_code_name,
                'discipline_code_status': self.business_metadata.discipline_code_status,
                'mark_name': self.business_metadata.mark_name,
                'mark_status': self.business_metadata.mark_status,
                'title': self.business_metadata.title,
                'title_status': self.business_metadata.title_status,
                'version_name': self.business_metadata.version_name,
                'version_status': self.business_metadata.version_status,
                'language_name': self.business_metadata.language_name,
                'language_status': self.business_metadata.language_status,
                'gip_name': self.business_metadata.gip_name,
                'gip_status': self.business_metadata.gip_status,
                'developer_name': self.business_metadata.developer_name,
                'developer_status': self.business_metadata.developer_status,
                'region_name': self.business_metadata.region_name,
                'region_status': self.business_metadata.region_status,
                'status_name': self.business_metadata.status_name,
                'status_status': self.business_metadata.status_status,
            }
        
        return result

    def is_valid(self) -> bool:
        return (self.word_count > 0 and 
                len(self.text) > 0 and 
                self.metadata.file_path is not None)
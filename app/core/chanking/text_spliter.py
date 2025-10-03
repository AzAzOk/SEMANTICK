class TextSplitter:

    """"Разбиение текста на чанки"""


    def __init__(self, chunk_size: int = 350, overlap: int = 50):
        self.chunk_size = chunk_size
        self.overlap = overlap
    

    def split_text(self, text: str) -> list[str]:

        """Разбивает текст на чанки с учетом перекрытия"""

        if not text:
            return []
        
        words = text.split()
        chunks = []
        start = 0
        text_length = len(words)
        
        while start < text_length:
            end = min(start + self.chunk_size, text_length)
            chunk = ' '.join(words[start:end])
            if self.validate_chunk(chunk):
                chunks.append(chunk)
            if end == text_length:
                break
            start += self.chunk_size - self.overlap
        
        return chunks
    

    def validate_chunk(self, chunk: str) -> bool:

        """Валидирует чанк текста"""

        if not chunk or not isinstance(chunk, str):
            return False
        if len(chunk.split()) > self.chunk_size + self.overlap:
            return False
        return True
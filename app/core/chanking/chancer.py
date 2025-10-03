from .chunk_models import DocumentChunkData


class DocumentChunker:

    """Сохдание объекта из чанка"""

    def __init__(self, text: list[str]):
        self.text = text
        self.data_for_chank = DocumentChunkData
        self.unit_chank = []


    def uniter(self):

        for i in range(len(self.text)):
            self.data_for_chank.text = self.text[i]
            self.unit_chank.append(self.data_for_chank.to_dict())




from sentence_transformers import SentenceTransformer
import uuid

class EmbeddingModel:

    """Класс для работы с моделью эмбеддингов"""

    def __init__(self, model_name="C:\\Users\\kulikovMA\\.ollama\\models\\manifests\\registry.ollama.ai\\library\\bge-m3\567m"):
        self.model = SentenceTransformer(model_name)
        print(f"Модель {model_name} загружена")
    
    def get_embedding(self, text):

        """Преобразует текст в вектор"""

        return self.model.encode([text])[0].tolist()
    
    def get_embeddings_batch(self, texts):

        """Преобразует список текстов в векторы"""

        return self.model.encode(texts).tolist()

embedder = EmbeddingModel()
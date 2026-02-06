from typing import List
import requests
import logging
from .config import settings

logger = logging.getLogger(__name__)


class Embedder:
    """
    embedder через Ollama
    """

    def embed(self, texts: List[str]) -> List[List[float]]:
        embeddings: List[List[float]] = []

        for text in texts:
            embedding = self._get_embedding(text)
            embeddings.append(embedding)

        return embeddings

    def _get_embedding(self, text: str) -> List[float]:
        response = requests.post(
            f"{settings.OLLAMA_URL}/api/embeddings",
            json={
                "model": settings.OLLAMA_MODEL,
                "prompt": text
            },
            timeout=60
        )

        if response.status_code != 200:
            logger.error(f"Ollama error: {response.text}")
            raise RuntimeError(f"Ollama embedding error")

        return response.json()["embedding"]

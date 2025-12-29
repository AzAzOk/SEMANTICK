import os

# ===============================
# REDIS
# ===============================
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# ===============================
# QDRENT
# ===============================

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
COLLECTION_NAME = "document_archive"

# ===============================
# OLLAMA
# ===============================

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = "bge-m3:567m"
MODEL_NAME = "C:\\Users\\kulikovMA\\.ollama\\models\\manifests\\registry.ollama.ai\\library\\bge-m3\567m"
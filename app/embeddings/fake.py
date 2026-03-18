"""
Deterministic fake embeddings for MT-IT / E2E tests.
No external API keys; same text always yields the same vector.
Used when RAG_FAKE_EMBEDDINGS=1 to validate tenant routing without paid services.
"""
import hashlib
from typing import List

# Dimension compatible with common pgvector indexes (e.g. 384 or 1536)
FAKE_EMBEDDING_DIM = 384


def _text_to_vector(text: str, dimension: int = FAKE_EMBEDDING_DIM) -> List[float]:
    """Produce a deterministic float vector from text (hash-based)."""
    h = hashlib.sha256(text.encode("utf-8")).digest()
    # Use bytes to get reproducible floats in [0, 1]; repeat to fill dimension
    out = []
    for i in range(dimension):
        b = h[i % len(h)]
        out.append((b / 255.0) - 0.5)  # [-0.5, 0.5] for stability
    return out


class FakeEmbeddings:
    """LangChain-compatible embeddings that return deterministic vectors from hashing."""

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [_text_to_vector(t) for t in texts]

    def embed_query(self, text: str) -> List[float]:
        return _text_to_vector(text)

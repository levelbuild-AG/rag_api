# tests/conftest.py
import os

from app.services.vector_store.async_pg_vector import AsyncPgVector

# Set environment variables early so config picks up test settings.
os.environ["TESTING"] = "1"
# Set DB_HOST (and DSN) to dummy values to avoid real connection attempts.
os.environ["DB_HOST"] = "localhost"  # or any dummy value
os.environ["DSN"] = "dummy://"

# -- Patch the vector store classes to bypass DB connection --

# Do this *before* importing any app modules.
from langchain_community.vectorstores.pgvector import PGVector

def dummy_post_init(self):
    # Skip extension creation
    pass

AsyncPgVector.__post_init__ = dummy_post_init
PGVector.__post_init__ = dummy_post_init

from langchain_core.documents import Document

class DummyVectorStore:
    def get_all_ids(self) -> list[str]:
        return ["testid1", "testid2"]

    def get_filtered_ids(self, ids) -> list[str]:
        dummy_ids = ["testid1", "testid2"]
        return [id for id in dummy_ids if id in ids]

    def get_documents_by_ids(self, ids: list[str]) -> list[Document]:
        """Sync version used by route when vector_store is not AsyncPgVector."""
        return [
            Document(page_content="Test content", metadata={"file_id": id})
            for id in ids
        ]

    async def get_documents_by_ids_async(self, ids: list[str]) -> list[Document]:
        return self.get_documents_by_ids(ids)

    def similarity_search_with_score_by_vector(self, embedding, k: int, filter: dict):
        doc = Document(
            page_content="Queried content",
            metadata={"file_id": filter.get("file_id", "testid1"), "user_id": "testuser"},
        )
        return [(doc, 0.9)]

    def add_documents(self, docs, ids):
        return ids

    async def aadd_documents(self, docs, ids):
        return ids

    def delete(self, ids=None, collection_only: bool = False):
        """Sync version used by route when vector_store is not AsyncPgVector."""
        return None

    async def delete_async(self, ids=None, collection_only: bool = False):
        return None

    # Implement the missing as_retriever() method
    def as_retriever(self):
        # Return self or wrap with a dummy retriever if needed.
        return self


# When TESTING=1, app.config sets vector_store = None. Patch it with a dummy so test_main and
# other tests that use vector_store get an object with embedding_function.
class DummyEmbedding:
    def embed_query(self, query):
        return [0.1, 0.2, 0.3]


_dummy_store = DummyVectorStore()
_dummy_store.embedding_function = DummyEmbedding()

import app.config as _config
_config.vector_store = _dummy_store
_config.retriever = _dummy_store.as_retriever()

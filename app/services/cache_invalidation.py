"""
Cache invalidation hooks for tenant configuration updates (Phase B).

When tenant RAG configuration is updated at runtime, call these functions
to invalidate cached vector stores and force re-resolution on next request.
"""

from app.services.tenant_vector_store_pool import get_tenant_vector_store_pool
from app.config import logger


def invalidate_tenant_rag_cache(tenant_id: str):
    """
    Invalidate cached vector store for a tenant after RAG config update.
    
    This is a Phase B hook - call this when tenant.config.rag.postgresUri
    or tenant.config.rag.vectorDbType is updated at runtime.
    
    Args:
        tenant_id: Tenant ID to invalidate
    """
    try:
        pool = get_tenant_vector_store_pool()
        pool.invalidate_tenant(tenant_id)
        logger.info(f"Invalidated RAG cache for tenant '{tenant_id}'")
    except Exception as e:
        logger.error(f"Error invalidating RAG cache for tenant '{tenant_id}': {e}")
        raise

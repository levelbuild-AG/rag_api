"""
Helper utilities for getting tenant-specific vector stores in routes.
"""

from fastapi import Request, HTTPException, status
from app.services.tenant_vector_store_pool import get_tenant_vector_store_pool
from app.config import logger


async def get_tenant_vector_store(request: Request):
    """
    Get tenant-specific vector store from request context.
    
    Extracts tenant_id from request.state (set by middleware) and returns
    the corresponding vector store instance from the pool.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        Vector store instance for the tenant
        
    Raises:
        HTTPException: If tenant_id is missing or vector store cannot be created
    """
    tenant_id = getattr(request.state, "tenant_id", None)
    
    if not tenant_id:
        logger.error("get_tenant_vector_store called but tenant_id not in request.state")
        raise HTTPException(
            status_code=500,
            detail="Internal error: tenant context not available. Ensure X-Tenant-ID header is set."
        )
    
    try:
        pool = get_tenant_vector_store_pool()
        return await pool.get_vector_store(tenant_id)
    except ValueError as e:
        # Tenant has no RAG config - fail hard
        logger.error(f"Failed to get vector store for tenant '{tenant_id}': {e}")
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting vector store for tenant '{tenant_id}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize vector store for tenant: {str(e)}"
        )

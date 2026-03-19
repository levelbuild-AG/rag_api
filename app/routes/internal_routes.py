"""
Internal API for cache invalidation (called by LibreChat API after tenant config updates).

Protected by X-Internal-Auth header (shared secret). Not exposed to public clients.
"""

import os
from fastapi import APIRouter, Header, HTTPException, Query, Body, Depends

from app.config import logger
from app.services.cache_invalidation import invalidate_tenant_rag_cache
from app.services.tenant_vector_store_pool import get_tenant_vector_store_pool

INTERNAL_AUTH_SECRET = os.getenv("RAG_INTERNAL_AUTH_SECRET") or os.getenv("INTERNAL_AUTH_SECRET")

router = APIRouter(prefix="/internal", tags=["internal"])


def verify_internal_auth(x_internal_auth: str = Header(None, alias="X-Internal-Auth")):
    if not INTERNAL_AUTH_SECRET or len(INTERNAL_AUTH_SECRET) < 16:
        raise HTTPException(status_code=503, detail="Internal API not configured")
    if not x_internal_auth or x_internal_auth != INTERNAL_AUTH_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden: invalid or missing internal auth")


@router.post("/cache/invalidate")
async def invalidate_tenant_cache(
    tenant_id: str = Query(None, alias="tenantId"),
    body: dict = Body(None),
    _: None = Depends(verify_internal_auth),
):
    """
    Invalidate RAG cache for a tenant. Called by LibreChat API after tenant config update (RAG fields changed).

    Query: ?tenantId=...  OR  Body: { "tenantId": "..." }
    """
    tid = tenant_id or (body and body.get("tenantId"))
    if not tid or not str(tid).strip():
        raise HTTPException(status_code=400, detail="tenantId required (query or body)")
    tid = str(tid).strip().lower()
    try:
        invalidate_tenant_rag_cache(tid)
        return {"ok": True, "tenantId": tid, "message": "Cache invalidated"}
    except Exception as e:
        logger.exception("Error invalidating RAG cache for tenant %s", tid)
        raise HTTPException(status_code=500, detail="Invalidation failed") from e


@router.post("/cache/invalidate_all")
async def invalidate_all_caches(
    _: None = Depends(verify_internal_auth),
):
    """Invalidate all tenant RAG caches (admin-only internal)."""
    try:
        pool = get_tenant_vector_store_pool()
        tenant_ids = list(pool._stores.keys())
        for tid in tenant_ids:
            pool.invalidate_tenant(tid)
        return {"ok": True, "invalidated": len(tenant_ids), "tenantIds": tenant_ids}
    except Exception as e:
        logger.exception("Error invalidating all RAG caches")
        raise HTTPException(status_code=500, detail="Invalidation failed") from e

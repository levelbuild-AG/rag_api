"""
Tenant-specific vector store pool manager.

Maintains per-tenant vector store instances, routing requests to tenant-specific
Postgres databases (or other vector stores) based on tenant configuration.
"""

import os
import asyncio
from typing import Optional, Dict, Any
from app.config import logger, embeddings, VECTOR_DB_TYPE, VectorDBType
from app.services.vector_store.factory import get_vector_store
from app.services.tenant_config import get_tenant_config_service
from app.services.database import PSQLDatabase


class TenantVectorStorePool:
    """
    Pool manager for tenant-specific vector stores.
    
    Each tenant gets its own vector store instance connected to tenant-specific
    Postgres database (or other vector store backend).
    
    Supports:
    - Internal Postgres DBs (Docker network hostnames)
    - External managed Postgres DBs
    - Connection pooling per tenant
    - Cache invalidation (Phase B hook stub)
    """
    
    def __init__(self):
        # tenantId -> vector_store instance
        self._stores: Dict[str, Any] = {}
        # tenantId -> postgres pool (for pgvector)
        self._pools: Dict[str, Any] = {}
        # tenantId -> last_access_time (for LRU eviction)
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._config_service = get_tenant_config_service()
        
        # Maximum number of tenant vector stores to cache (env-configurable)
        self._max_stores = int(os.getenv("MAX_TENANT_VECTOR_STORES", "200"))
        
        logger.info(
            f"Initialized TenantVectorStorePool (max stores: {self._max_stores})"
        )
    
    async def get_vector_store(self, tenant_id: str):
        """
        Get or create vector store for a tenant.
        
        Args:
            tenant_id: Tenant ID (required)
            
        Returns:
            Vector store instance for the tenant
            
        Raises:
            ValueError: If tenant_id is missing or tenant has no RAG config
        """
        if not tenant_id:
            raise ValueError("Tenant ID is required for vector store access")
        
        # Check cache first
        if tenant_id in self._stores:
            # Update access time for LRU
            self._access_times[tenant_id] = asyncio.get_event_loop().time()
            return self._stores[tenant_id]
        
        async with self._lock:
            # Double-check after acquiring lock
            if tenant_id in self._stores:
                self._access_times[tenant_id] = asyncio.get_event_loop().time()
                return self._stores[tenant_id]
            
            # Evict oldest if cache is full
            if len(self._stores) >= self._max_stores:
                await self._evict_oldest()
            
            # Get tenant RAG config
            rag_config = self._config_service.get_tenant_rag_config(tenant_id)
            if not rag_config:
                raise ValueError(
                    f"Tenant '{tenant_id}' has no RAG configuration. "
                    "RAG features require tenant.config.rag.postgresUri to be set."
                )
            
            postgres_uri = rag_config["postgresUri"]
            vector_db_type = rag_config.get("vectorDbType", "pgvector")
            
            logger.info(
                f"Creating vector store for tenant '{tenant_id}' "
                f"(type: {vector_db_type}, URI: {self._mask_uri(postgres_uri)})"
            )
            
            # Create vector store based on type
            if vector_db_type == "pgvector":
                # For pgvector, we need to create both the pool and vector store
                # Use the same collection name for all tenants (per tenant DB)
                collection_name = os.getenv("COLLECTION_NAME", "testcollection")
                
                # Create connection string for langchain (postgresql+psycopg2://)
                # Convert postgres:// or postgresql:// to postgresql+psycopg2:// as needed.
                # CRITICAL: Preserve query parameters (e.g., ?sslmode=require) for external managed DBs
                if postgres_uri.startswith("postgresql+psycopg2://"):
                    connection_string = postgres_uri  # Already in correct format
                elif postgres_uri.startswith("postgresql://"):
                    # Replace only the scheme, preserving query params
                    connection_string = postgres_uri.replace(
                        "postgresql://", "postgresql+psycopg2://", 1
                    )
                elif postgres_uri.startswith("postgres://"):
                    # Older-style postgres:// URIs: normalize to postgresql+psycopg2://
                    connection_string = postgres_uri.replace(
                        "postgres://", "postgresql+psycopg2://", 1
                    )
                else:
                    # Fallback: attempt to normalize any lingering postgresql:// prefix
                    connection_string = postgres_uri.replace(
                        "postgresql://", "postgresql+psycopg2://", 1
                    )
                
                vector_store = get_vector_store(
                    connection_string=connection_string,
                    embeddings=embeddings,
                    collection_name=collection_name,
                    mode="async",
                )
                
                # Store both vector store and connection info
                self._stores[tenant_id] = vector_store
                self._pools[tenant_id] = {
                    "postgresUri": postgres_uri,
                    "connectionString": connection_string,
                }
                
            elif vector_db_type == "atlas-mongo":
                # For Atlas MongoDB, use the connection string directly
                collection_name = os.getenv("COLLECTION_NAME", "testcollection")
                search_index = os.getenv("ATLAS_SEARCH_INDEX", "vector_index")
                
                vector_store = get_vector_store(
                    connection_string=postgres_uri,  # Actually MongoDB URI
                    embeddings=embeddings,
                    collection_name=collection_name,
                    mode="atlas-mongo",
                    search_index=search_index,
                )
                
                self._stores[tenant_id] = vector_store
            else:
                raise ValueError(
                    f"Unsupported vector DB type '{vector_db_type}' for tenant '{tenant_id}'"
                )
            
            # Record access time
            self._access_times[tenant_id] = asyncio.get_event_loop().time()
            
            logger.info(f"Created vector store for tenant '{tenant_id}'")
            return self._stores[tenant_id]
    
    async def _evict_oldest(self):
        """
        Evict the least recently used tenant vector store.
        Called when cache reaches MAX_TENANT_VECTOR_STORES.
        """
        if not self._access_times:
            return
        
        # Find tenant with oldest access time
        oldest_tenant = min(self._access_times.items(), key=lambda x: x[1])[0]
        
        logger.info(
            f"Evicting oldest tenant vector store (cache full: {len(self._stores)}/{self._max_stores}): {oldest_tenant}"
        )
        
        # Remove from cache
        if oldest_tenant in self._stores:
            del self._stores[oldest_tenant]
        if oldest_tenant in self._pools:
            del self._pools[oldest_tenant]
        if oldest_tenant in self._access_times:
            del self._access_times[oldest_tenant]
    
    def invalidate_tenant(self, tenant_id: str):
        """
        Invalidate cached vector store for a tenant (Phase B hook).
        
        Call this when tenant RAG config is updated at runtime.
        
        Args:
            tenant_id: Tenant ID to invalidate
        """
        if tenant_id in self._stores:
            logger.info(f"Invalidating vector store cache for tenant '{tenant_id}'")
            # TODO: Close connections/pools if needed (Phase B)
            del self._stores[tenant_id]
            if tenant_id in self._pools:
                del self._pools[tenant_id]
            if tenant_id in self._access_times:
                del self._access_times[tenant_id]
    
    def _mask_uri(self, uri: str) -> str:
        """Mask sensitive parts of URI for logging."""
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(uri)
            if parsed.password:
                masked = parsed._replace(
                    netloc=f"{parsed.username}:***@{parsed.hostname}:{parsed.port or ''}"
                )
                return urlunparse(masked)
        except Exception:
            pass
        return "***"


# Singleton instance
_tenant_pool: Optional[TenantVectorStorePool] = None


def get_tenant_vector_store_pool() -> TenantVectorStorePool:
    """Get singleton TenantVectorStorePool instance."""
    global _tenant_pool
    if _tenant_pool is None:
        _tenant_pool = TenantVectorStorePool()
    return _tenant_pool

"""
Tenant configuration service for rag_api.

Reads tenant-specific RAG configuration (Postgres URI, vector DB type) from system MongoDB.
Uses direct MongoDB connection (Pattern 1) - no HTTP roundtrip to LibreChat API.
"""

import os
from typing import Optional, Dict, Any
from pymongo import MongoClient
from app.config import logger


class TenantConfigService:
    """
    Service for reading tenant configuration from system MongoDB.
    
    Reads tenant.config.rag.postgresUri and tenant.config.rag.vectorDbType
    from the system MongoDB database (not tenant-specific DBs).
    """
    
    def __init__(self):
        # System MongoDB URI - where Tenant collection is stored
        # This is separate from tenant-specific MongoDB databases
        system_mongo_uri = os.getenv("SYSTEM_MONGO_URI")
        if not system_mongo_uri:
            raise ValueError(
                "SYSTEM_MONGO_URI environment variable is required for tenant config lookup"
            )
        
        # Create MongoDB client with connection pooling and timeouts
        # Use singleton pattern - client is reused across requests
        self.client = MongoClient(
            system_mongo_uri,
            serverSelectionTimeoutMS=5000,  # 5 second timeout for server selection
            connectTimeoutMS=10000,  # 10 second connection timeout
            socketTimeoutMS=30000,  # 30 second socket timeout
            maxPoolSize=10,  # Connection pool size
        )
        # Extract database name from URI or use default
        # MongoDB URI format: mongodb://host:port/dbname
        db_name = os.getenv("SYSTEM_MONGO_DB")
        if not db_name:
            # Try to extract from URI
            from urllib.parse import urlparse
            parsed = urlparse(system_mongo_uri)
            db_name = parsed.path.lstrip('/') if parsed.path else 'LibreChat'
        self.db = self.client[db_name]
        self.tenants_collection = self.db.tenants
        
        logger.info("Initialized TenantConfigService with system MongoDB")
    
    def get_tenant_rag_config(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """
        Get RAG configuration for a tenant.
        
        Args:
            tenant_id: Tenant ID (lowercase)
            
        Returns:
            Dict with 'postgresUri' and optionally 'vectorDbType', or None if tenant not found
            or RAG config not set.
            
        Raises:
            ValueError: If tenant is not active
        """
        tenant = self.tenants_collection.find_one(
            {"tenantId": tenant_id.lower(), "status": "active"}
        )
        
        if not tenant:
            logger.warning(f"Tenant '{tenant_id}' not found or not active")
            return None
        
        config = tenant.get("config", {})
        rag_config = config.get("rag")
        
        if not rag_config:
            logger.debug(f"Tenant '{tenant_id}' has no RAG configuration")
            return None
        
        postgres_uri = rag_config.get("postgresUri")
        if not postgres_uri:
            logger.warning(f"Tenant '{tenant_id}' has RAG config but no postgresUri")
            return None
        
        return {
            "postgresUri": postgres_uri,
            "vectorDbType": rag_config.get("vectorDbType", "pgvector"),
        }
    
    def close(self):
        """Close MongoDB connection."""
        self.client.close()


# Singleton instance
_tenant_config_service: Optional[TenantConfigService] = None


def get_tenant_config_service() -> TenantConfigService:
    """Get singleton TenantConfigService instance."""
    global _tenant_config_service
    if _tenant_config_service is None:
        _tenant_config_service = TenantConfigService()
    return _tenant_config_service

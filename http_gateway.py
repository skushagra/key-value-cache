#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
import uvicorn
import asyncio

# Import the AsyncCacheSDK from our SDK module
from sdk.cache_sdk import AsyncCacheSDK

# Define API models
class Item(BaseModel):
    value: str = Field(..., description="The value to store")
    ttl: Optional[int] = Field(None, description="Time to live in seconds")

class ItemResponse(BaseModel):
    key: str
    value: str
    success: bool

# Cache client singleton for dependency injection
class CacheClientDependency:
    client: Optional[AsyncCacheSDK] = None
    
    async def initialize(self):
        if self.client is None:
            self.client = AsyncCacheSDK(
                host="127.0.0.1", 
                port=7171, 
                timeout=1.0, 
                pool_size=10
            )
        return self.client
    
    async def get_client(self):
        return await self.initialize()
    
    async def close(self):
        if self.client:
            await self.client.close()
            self.client = None

cache_dependency = CacheClientDependency()

# Create FastAPI application
app = FastAPI(
    title="KV Cache API",
    description="REST API for the high-performance key-value cache",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency for getting the cache client
async def get_cache_client():
    client = await cache_dependency.get_client()
    return client

# Define endpoints
@app.get("/")
async def root():
    return {"message": "KV Cache API is running"}

@app.get("/health")
async def health(cache: AsyncCacheSDK = Depends(get_cache_client)):
    try:
        # Set and get a key to verify cache is working
        test_key = "health_check"
        test_value = "ok"
        set_success = await cache.set(test_key, test_value)
        if set_success:
            value = await cache.get(test_key)
            return {
                "status": "healthy",
                "cache": "connected" if value == test_value else "error"
            }
        return {"status": "degraded", "cache": "error"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/{key}", response_model=ItemResponse)
async def get_item(key: str, cache: AsyncCacheSDK = Depends(get_cache_client)):
    value = await cache.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"key": key, "value": value, "success": True}

@app.post("/{key}", response_model=ItemResponse)
async def set_item(key: str, item: Item, cache: AsyncCacheSDK = Depends(get_cache_client)):
    success = await cache.set(key, item.value)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to store the item")
    return {"key": key, "value": item.value, "success": success}

if __name__ == "__main__":
    # Run with uvicorn
    print("Starting FastAPI server with KV cache integration")
    uvicorn.run(
        "fastapi_example:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=False,
        workers=4,
        log_level="info"
    )
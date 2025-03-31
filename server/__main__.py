import asyncio
import struct
import mmh3
import time
import signal
import functools
import logging
import os
from typing import Dict, Tuple, Optional, Any, List
from concurrent.futures import ThreadPoolExecutor
import uvloop

# Configuration constants
MAX_CACHE_SIZE = 1_000_000
MAX_KEY_SIZE = 256
MAX_VALUE_SIZE = 256
BIND_IP = "0.0.0.0"
BIND_PORT = 7171
CACHE_SHARDS = 1024  # Increased for better parallelism
# Use CPU count for workers, with a minimum of 8
READ_WORKERS = max(8, os.cpu_count() or 8)
RESPONSE_CACHE_MAX_SIZE = 10000
RESPONSE_CACHE_TTL = 60  # seconds

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache structures
class CacheShardManager:
    def __init__(self, num_shards: int):
        self.shards: List[Dict[str, Tuple[str, float]]] = [{} for _ in range(num_shards)]
        self.write_locks: List[asyncio.Lock] = [asyncio.Lock() for _ in range(num_shards)]
        # LRU response cache with TTL - stores (response_bytes, timestamp)
        self.response_cache: Dict[str, Tuple[bytes, float]] = {}
        self.response_cache_lock = asyncio.Lock()
        
    def get_shard_index(self, key: str) -> int:
        """Fast shard index calculation using MurmurHash3"""
        return mmh3.hash(key, seed=0) % len(self.shards)
    
    def _get_from_shard(self, shard_idx: int, key: str) -> Optional[str]:
        """Direct synchronous access to cache shard with TTL support"""
        shard = self.shards[shard_idx]
        item = shard.get(key)
        if item is None:
            return None
        
        value, _ = item
        return value
    
    async def get_item(self, key: str) -> Tuple[str, bool, Optional[bytes]]:
        """Optimized get operation with response caching"""
        current_time = time.time()
        
        # Check response cache first for hot paths
        async with self.response_cache_lock:
            cached_response = self.response_cache.get(key)
            if cached_response:
                response_bytes, timestamp = cached_response
                if current_time - timestamp < RESPONSE_CACHE_TTL:
                    return key, True, response_bytes
                else:
                    # Expired entry
                    del self.response_cache[key]
        
        # Get from main cache
        shard_idx = self.get_shard_index(key)
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(
            None,  # Use default executor
            functools.partial(self._get_from_shard, shard_idx, key)
        )
        
        if value is not None:
            # Cache response for hot keys
            serialized_response = serialize_response(value)
            
            async with self.response_cache_lock:
                # Prune cache if too large (simple LRU by time)
                if len(self.response_cache) >= RESPONSE_CACHE_MAX_SIZE:
                    oldest_key = min(self.response_cache.items(), 
                                    key=lambda x: x[1][1])[0]
                    del self.response_cache[oldest_key]
                
                # Store in response cache with timestamp
                self.response_cache[key] = (serialized_response, current_time)
                
            return value, False, serialized_response
            
        return "Key not found", False, KEY_NOT_FOUND_RESPONSE
    
    async def put_item(self, key: str, value: str) -> bool:
        """Store an item in the cache with TTL"""
        if len(key) > MAX_KEY_SIZE or len(value) > MAX_VALUE_SIZE:
            return False
        
        shard_idx = self.get_shard_index(key)
        current_time = time.time()
        
        async with self.write_locks[shard_idx]:
            self.shards[shard_idx][key] = (value, current_time)
            
            # Invalidate response cache
            async with self.response_cache_lock:
                if key in self.response_cache:
                    del self.response_cache[key]
            
            return True
    
    async def delete_item(self, key: str) -> bool:
        """Delete an item from the cache"""
        shard_idx = self.get_shard_index(key)
        
        async with self.write_locks[shard_idx]:
            if key in self.shards[shard_idx]:
                del self.shards[shard_idx][key]
                
                # Invalidate response cache
                async with self.response_cache_lock:
                    if key in self.response_cache:
                        del self.response_cache[key]
                
                return True
        
        return False
    
    async def cleanup_expired_entries(self):
        """Periodically cleanup expired entries"""
        while True:
            try:
                current_time = time.time()
                
                # Clean response cache
                async with self.response_cache_lock:
                    expired_keys = []
                    for key, (_, timestamp) in self.response_cache.items():
                        if current_time - timestamp > RESPONSE_CACHE_TTL:
                            expired_keys.append(key)
                            
                    for key in expired_keys:
                        del self.response_cache[key]
                
                # Wait before next cleanup
                await asyncio.sleep(10)  # Run cleanup every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(10)  # Wait before retrying

# Pre-computed responses for performance
INVALID_REQ_RESPONSE = None
STORED_RESPONSE = "Stored"
FAILED_RESPONSE = "Failed"
KEY_NOT_FOUND_RESPONSE = None
DELETED_RESPONSE = None
UNKNOWN_CMD_RESPONSE = None

# Initialize the cache manager
cache_manager = CacheShardManager(CACHE_SHARDS)

def serialize_response(message: str) -> bytes:
    """Fast binary response serialization"""
    message_bytes = message.encode()
    return struct.pack(f"!H{len(message_bytes)}s", len(message_bytes), message_bytes)

async def recvall_async(reader, n: int) -> Optional[bytes]:
    """Efficient async data reception with error handling"""
    try:
        return await reader.readexactly(n)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None

async def deserialize_request_async(reader) -> Optional[Tuple]:
    """Optimized binary protocol parsing"""
    try:
        # Fast path - read header in a single operation
        header = await recvall_async(reader, 3)
        if not header or len(header) < 3:
            return None
            
        command, key_len = struct.unpack("!BH", header)
        
        # Read key data
        key_data = await recvall_async(reader, key_len)
        if not key_data or len(key_data) < key_len:
            return None
        
        key = key_data.decode()
        
        # Fast path for get operations (most common)
        if command == 1:  # Get command
            return ("get", key)
        elif command == 2:  # Set command
            value_len_data = await recvall_async(reader, 2)
            if not value_len_data:
                return None
                
            value_len = struct.unpack("!H", value_len_data)[0]
            
            value_data = await recvall_async(reader, value_len)
            if not value_data:
                return None
                
            value = value_data.decode()
            return ("set", key, value)
        elif command == 3:  # Delete command
            return ("delete", key)
        else:
            return None
    except Exception as e:
        logger.debug(f"Error deserializing request: {e}")
        return None

async def handle_client_async(reader, writer):
    """Optimized client request handler"""
    try:
        # Fast path for requests
        command = await deserialize_request_async(reader)
        
        if not command:
            response_data = INVALID_REQ_RESPONSE
        else:
            try:
                if command[0] == "get":
                    # Optimized get path
                    value, cached, serialized_response = await cache_manager.get_item(command[1])
                    if cached or serialized_response:
                        response_data = serialized_response
                    else:
                        response_data = KEY_NOT_FOUND_RESPONSE
                elif command[0] == "set":
                    success = await cache_manager.put_item(command[1], command[2])
                    response_data = STORED_RESPONSE if success else FAILED_RESPONSE
                elif command[0] == "delete":
                    success = await cache_manager.delete_item(command[1])
                    response_data = DELETED_RESPONSE if success else KEY_NOT_FOUND_RESPONSE
                else:
                    response_data = UNKNOWN_CMD_RESPONSE
            except Exception as e:
                logger.debug(f"Error handling command {command[0]}: {e}")
                response_data = UNKNOWN_CMD_RESPONSE
        
        # Zero-copy write and drain for maximum performance
        writer.write(response_data)
        await writer.drain()
            
    except Exception as e:
        logger.debug(f"Connection handling error: {e}")
    finally:
        # Proper connection closing with error handling
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def startup():
    """Initialize the server and precalculate responses"""
    global INVALID_REQ_RESPONSE, STORED_RESPONSE, FAILED_RESPONSE
    global KEY_NOT_FOUND_RESPONSE, DELETED_RESPONSE, UNKNOWN_CMD_RESPONSE
    
    # Precalculate common responses
    INVALID_REQ_RESPONSE = serialize_response("Invalid request")
    STORED_RESPONSE = serialize_response("Stored")
    FAILED_RESPONSE = serialize_response("Failed")
    KEY_NOT_FOUND_RESPONSE = serialize_response("Key not found")
    DELETED_RESPONSE = serialize_response("Deleted")
    UNKNOWN_CMD_RESPONSE = serialize_response("Unknown command")
    
    # Start background tasks
    asyncio.create_task(cache_manager.cleanup_expired_entries())
    
    logger.info(f"Server initialized with {CACHE_SHARDS} shards and {READ_WORKERS} workers")

async def main():
    # Install uvloop for maximum performance
    uvloop.install()
    
    # Initialize server
    await startup()
    
    # Create server with optimized parameters
    server = await asyncio.start_server(
        handle_client_async, 
        BIND_IP, 
        BIND_PORT,
        backlog=10000,  # Increased for higher concurrency
        reuse_address=True,
        start_serving=True,
        limit=65536,    # Larger buffer for fewer syscalls
    )
    
    logger.info(f"High-performance KV cache server listening on {BIND_IP}:{BIND_PORT}")
    
    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: asyncio.create_task(shutdown(signame, server))
        )
    
    async with server:
        await server.serve_forever()

async def shutdown(signame, server):
    """Graceful shutdown sequence"""
    logger.info(f"Received {signame}, shutting down...")
    
    # Close server
    server.close()
    await server.wait_closed()
    
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Shutdown complete")
    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server interrupted by keyboard")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
import socket
import struct
import time
from typing import Optional, Union, Any, Dict, List, Tuple
import random
import asyncio
from concurrent.futures import ThreadPoolExecutor


class CacheSDK:
    
    def __init__(self, host: str = "127.0.0.1", port: int = 7171, timeout: float = 0.5,
                 pool_size: int = 5, max_retries: int = 3):
        """Initialize the cache client.
        
        Args:
            host: Host of the cache server
            port: Port of the cache server
            timeout: Timeout for requests in seconds
            pool_size: Size of the connection pool
            max_retries: Maximum number of retries for failed operations
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self._connection_pool = self._create_connection_pool(pool_size)
        self._pool_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=pool_size)
    
    def _create_connection_pool(self, size: int) -> List[Optional[socket.socket]]:
        """Initialize a connection pool of the specified size."""
        return [None] * size
    
    def _get_connection(self) -> Tuple[int, socket.socket]:
        """Get a connection from the pool or create a new one."""
        for i, conn in enumerate(self._connection_pool):
            if conn is not None and not self._is_socket_closed(conn):
                return i, conn
                
        # Create a new connection
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((self.host, self.port))
        conn.settimeout(self.timeout)
        
        # Find an empty slot
        for i, existing_conn in enumerate(self._connection_pool):
            if existing_conn is None or self._is_socket_closed(existing_conn):
                self._connection_pool[i] = conn
                return i, conn
        
        # If no empty slots, replace a random connection
        i = random.randint(0, len(self._connection_pool) - 1)
        try:
            self._connection_pool[i].close()
        except:
            pass
        self._connection_pool[i] = conn
        return i, conn
    
    def _is_socket_closed(self, sock: socket.socket) -> bool:
        """Check if a socket is closed or unusable."""
        try:
            # Check if the socket is readable with a 0 timeout
            # If it is and returns empty string, the connection was closed
            sock.settimeout(0.0)
            data = sock.recv(1, socket.MSG_PEEK)
            sock.settimeout(self.timeout)
            return len(data) == 0
        except (socket.error, BlockingIOError):
            # BlockingIOError means the socket is open but not readable
            sock.settimeout(self.timeout)
            return False
        except:
            return True
    
    def _release_connection(self, index: int, error: bool = False):
        """Return a connection to the pool or close it if there was an error."""
        if error and self._connection_pool[index] is not None:
            try:
                self._connection_pool[index].close()
            except:
                pass
            self._connection_pool[index] = None
    
    def _serialize_get(self, key: str) -> bytes:
        """Serialize a GET command."""
        key_bytes = key.encode()
        return struct.pack(f"!BH{len(key_bytes)}s", 1, len(key_bytes), key_bytes)
    
    def _serialize_set(self, key: str, value: Any) -> bytes:
        """Serialize a SET command."""
        key_bytes = key.encode()
        value_bytes = str(value).encode()
        return struct.pack(f"!BH{len(key_bytes)}sH{len(value_bytes)}s", 
                          2, len(key_bytes), key_bytes, len(value_bytes), value_bytes)
    
    def _serialize_delete(self, key: str) -> bytes:
        """Serialize a DELETE command."""
        key_bytes = key.encode()
        return struct.pack(f"!BH{len(key_bytes)}s", 3, len(key_bytes), key_bytes)
    
    def _deserialize_response(self, response: bytes) -> str:
        """Deserialize server response."""
        if not response or len(response) < 2:
            return "Error: Empty response"
        
        try:
            message_len = struct.unpack("!H", response[:2])[0]
            return response[2:2+message_len].decode()
        except:
            return "Error: Invalid response"
    
    def _send_command(self, serialized_command: bytes) -> str:
        """Send command to server and get response with retries."""
        retries = 0
        last_error = None
        
        while retries < self.max_retries:
            conn_index = -1
            try:
                conn_index, conn = self._get_connection()
                conn.sendall(serialized_command)
                
                # Read the response
                header = conn.recv(2)
                if not header or len(header) < 2:
                    raise socket.error("Failed to read response header")
                
                message_len = struct.unpack("!H", header)[0]
                message_data = conn.recv(message_len)
                if len(message_data) < message_len:
                    raise socket.error("Incomplete read")
                
                response = header + message_data
                
                self._release_connection(conn_index)
                return self._deserialize_response(response)
                
            except Exception as e:
                retries += 1
                last_error = str(e)
                
                if conn_index >= 0:
                    self._release_connection(conn_index, error=True)
                
                # Sleep before retry with exponential backoff
                if retries < self.max_retries:
                    time.sleep(0.1 * (2 ** retries))
        
        return f"Error: {last_error}"
    
    def set(self, key: str, value: Any) -> bool:
        """Set a key-value pair in the cache.
        
        Args:
            key: The key to set
            value: The value to set
            
        Returns:
            True if key was set successfully, False otherwise
        """
        command = self._serialize_set(key, value)
        response = self._send_command(command)
        
        return response == "Stored"
    
    def get(self, key: str) -> Optional[str]:
        """Get a value from the cache.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        command = self._serialize_get(key)
        response = self._send_command(command)
        
        if response.startswith("Error") or response == "Key not found":
            return None
        return response
    
    def delete(self, key: str) -> bool:
        """Delete a key from the cache.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted successfully, False otherwise
        """
        command = self._serialize_delete(key)
        response = self._send_command(command)
        
        return response == "Deleted"
    
    def close(self):
        """Close all connections in the pool."""
        for i, conn in enumerate(self._connection_pool):
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass
                self._connection_pool[i] = None
        
        self._executor.shutdown(wait=False)


class AsyncCacheSDK:
    """Async version of the CacheSDK for use with asyncio."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 7171, timeout: float = 0.5,
                 pool_size: int = 10, max_retries: int = 3):
        """Initialize the async cache client.
        
        Args:
            host: Host of the cache server
            port: Port of the cache server
            timeout: Timeout for requests in seconds
            pool_size: Size of the connection pool
            max_retries: Maximum number of retries for failed operations
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.pool_size = pool_size
        self._connection_pool = []
        self._pool_lock = asyncio.Lock()
    
    async def _get_connection(self):
        """Get a connection from the pool or create a new one."""
        async with self._pool_lock:
            # Check for available connections
            for i, (reader, writer) in enumerate(self._connection_pool):
                if not writer.is_closing():
                    return i, reader, writer
            
            # Create a new connection if pool isn't full
            if len(self._connection_pool) < self.pool_size:
                reader, writer = await asyncio.open_connection(
                    self.host, self.port, limit=65536
                )
                self._connection_pool.append((reader, writer))
                return len(self._connection_pool) - 1, reader, writer
            
            # If pool is full, close a random connection and create a new one
            idx = random.randint(0, len(self._connection_pool) - 1)
            _, old_writer = self._connection_pool[idx]
            old_writer.close()
            await old_writer.wait_closed()
            
            reader, writer = await asyncio.open_connection(
                self.host, self.port, limit=65536
            )
            self._connection_pool[idx] = (reader, writer)
            return idx, reader, writer
    
    async def _release_connection(self, index, error=False):
        """Return a connection to the pool or close it if there was an error."""
        async with self._pool_lock:
            if index >= len(self._connection_pool):
                return
                
            if error:
                _, writer = self._connection_pool[index]
                writer.close()
                await writer.wait_closed()
                
                # Create a new connection
                try:
                    reader, writer = await asyncio.open_connection(
                        self.host, self.port, limit=65536
                    )
                    self._connection_pool[index] = (reader, writer)
                except:
                    # Remove the connection if we can't reconnect
                    self._connection_pool.pop(index)
    
    def _serialize_get(self, key: str) -> bytes:
        """Serialize a GET command."""
        key_bytes = key.encode()
        return struct.pack(f"!BH{len(key_bytes)}s", 1, len(key_bytes), key_bytes)
    
    def _serialize_set(self, key: str, value: Any) -> bytes:
        """Serialize a SET command."""
        key_bytes = key.encode()
        value_bytes = str(value).encode()
        return struct.pack(f"!BH{len(key_bytes)}sH{len(value_bytes)}s", 
                          2, len(key_bytes), key_bytes, len(value_bytes), value_bytes)
    
    def _serialize_delete(self, key: str) -> bytes:
        """Serialize a DELETE command."""
        key_bytes = key.encode()
        return struct.pack(f"!BH{len(key_bytes)}s", 3, len(key_bytes), key_bytes)
    
    async def _deserialize_response(self, reader):
        """Deserialize server response."""
        try:
            header = await reader.readexactly(2)
            message_len = struct.unpack("!H", header)[0]
            message = await reader.readexactly(message_len)
            return message.decode()
        except asyncio.IncompleteReadError:
            raise ConnectionError("Connection closed")
        except Exception as e:
            return f"Error: {e}"
    
    async def _send_command(self, serialized_command: bytes) -> str:
        """Send command to server and get response with retries."""
        retries = 0
        last_error = None
        conn_index = -1
        
        while retries < self.max_retries:
            try:
                conn_index, reader, writer = await self._get_connection()
                
                # Send the command
                writer.write(serialized_command)
                await writer.drain()
                
                # Read the response with timeout
                try:
                    response = await asyncio.wait_for(
                        self._deserialize_response(reader),
                        timeout=self.timeout
                    )
                    await self._release_connection(conn_index)
                    return response
                except (asyncio.TimeoutError, ConnectionError) as e:
                    # Mark connection as bad and get a new one
                    await self._release_connection(conn_index, error=True)
                    last_error = str(e)
                    retries += 1
                    continue
                
            except Exception as e:
                retries += 1
                last_error = str(e)
                
                if conn_index >= 0:
                    await self._release_connection(conn_index, error=True)
                
                # Sleep before retry with exponential backoff
                if retries < self.max_retries:
                    await asyncio.sleep(0.1 * (2 ** retries))
        
        return f"Error: {last_error}"
    
    async def set(self, key: str, value: Any) -> bool:
        """Set a key-value pair in the cache asynchronously.
        
        Args:
            key: The key to set
            value: The value to set
            
        Returns:
            True if key was set successfully, False otherwise
        """
        command = self._serialize_set(key, value)
        response = await self._send_command(command)
        
        return response == "Stored"
    
    async def get(self, key: str) -> Optional[str]:
        """Get a value from the cache asynchronously.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        command = self._serialize_get(key)
        response = await self._send_command(command)
        
        if response.startswith("Error") or response == "Key not found":
            return None
        return response
    
    async def delete(self, key: str) -> bool:
        """Delete a key from the cache asynchronously.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted successfully, False otherwise
        """
        command = self._serialize_delete(key)
        response = await self._send_command(command)
        
        return response == "Deleted"
    
    async def close(self):
        """Close all connections in the pool."""
        async with self._pool_lock:
            for _, writer in self._connection_pool:
                writer.close()
                await writer.wait_closed()
            self._connection_pool = []


if __name__ == "__main__":
    # Example usage of synchronous SDK
    cache = CacheSDK()
    
    # Set a value
    success = cache.set("user:1", "John Doe")
    print(f"Set success: {success}")
    
    # Get a value
    value = cache.get("user:1")
    print(f"Got value: {value}")
    
    # Get a non-existent value
    value = cache.get("nonexistent")
    print(f"Got non-existent value: {value}")
    
    # Example of async usage
    async def async_example():
        async_cache = AsyncCacheSDK()
        
        # Set a value
        success = await async_cache.set("async_user:1", "Jane Smith")
        print(f"Async set success: {success}")
        
        # Get a value
        value = await async_cache.get("async_user:1")
        print(f"Async got value: {value}")
        
        await async_cache.close()
    
    import asyncio
    asyncio.run(async_example())
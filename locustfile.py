import time
import random
import string
from locust import User, task, between, events
from sdk.cache_sdk import CacheSDK
from typing import Dict, Any

class CacheUser(User):
    wait_time = between(0.1, 1.0)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = None
        self.test_keys = []  # Track keys created during the test
    
    def on_start(self):
        """Initialize the cache client when a user starts."""
        self.cache = CacheSDK(host="127.0.0.1", port=7171, timeout=2.0, max_retries=2)
        # Pre-create some test keys
        for _ in range(5):
            key = self._generate_random_key()
            self.test_keys.append(key)
            self.cache.set(key, self._generate_random_value())
    
    def on_stop(self):
        """Clean up when user stops."""
        # Optionally cleanup keys
        for key in self.test_keys:
            try:
                self.cache.delete(key)
            except:
                pass
        self.cache.close()
    
    def _generate_random_key(self) -> str:
        """Generate a random key for testing."""
        prefix = random.choice(["user:", "product:", "session:", "cart:", "order:"])
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"{prefix}{suffix}"
    
    def _generate_random_value(self) -> str:
        """Generate a random value for testing."""
        # Generate values of different sizes
        size = random.choice([10, 50, 200, 1000])
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    
    @task(5)
    def set_key(self):
        """Set a key with a random value."""
        key = self._generate_random_key()
        value = self._generate_random_value()
        
        start_time = time.time()
        try:
            result = self.cache.set(key, value)
            if result:
                # Keep track of successful keys for future get/delete operations
                if len(self.test_keys) > 20:  # Limit the number of tracked keys
                    self.test_keys.pop(0)
                self.test_keys.append(key)
            
            events.request.fire(
                request_type="set",
                name="set_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=len(str(value)),
                exception=None,
                context={}
            )
        except Exception as e:
            events.request.fire(
                request_type="set",
                name="set_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={}
            )
    
    @task(10)
    def get_key(self):
        """Get a previously set key."""
        if not self.test_keys:
            # If no keys available, set one
            self.set_key()
            return
            
        key = random.choice(self.test_keys)
        
        start_time = time.time()
        try:
            result = self.cache.get(key)
            events.request.fire(
                request_type="get",
                name="get_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=len(str(result)) if result else 0,
                exception=None,
                context={}
            )
        except Exception as e:
            events.request.fire(
                request_type="get",
                name="get_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={}
            )
    
    @task(1)
    def get_nonexistent(self):
        """Try to get a key that doesn't exist."""
        key = f"nonexistent:{random.randint(1, 10000)}"
        
        start_time = time.time()
        try:
            result = self.cache.get(key)
            events.request.fire(
                request_type="get",
                name="get_nonexistent",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=None,
                context={}
            )
        except Exception as e:
            events.request.fire(
                request_type="get",
                name="get_nonexistent",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={}
            )
    
    @task(2)
    def delete_key(self):
        """Delete a previously set key."""
        if not self.test_keys:
            return
            
        key = random.choice(self.test_keys)
        self.test_keys.remove(key)  # Remove from our tracking list
        
        start_time = time.time()
        try:
            result = self.cache.delete(key)
            events.request.fire(
                request_type="delete",
                name="delete_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=None,
                context={}
            )
        except Exception as e:
            events.request.fire(
                request_type="delete",
                name="delete_key",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={}
            )
    
    @task(1)
    def mixed_operations(self):
        """Perform a mixture of operations in sequence."""
        # Create a new key
        key = self._generate_random_key()
        value = self._generate_random_value()
        
        # Track total time for the sequence
        start_time = time.time()
        success = True
        exception = None
        
        try:
            # Set the key
            set_result = self.cache.set(key, value)
            if not set_result:
                success = False
            
            # Get the key immediately after
            get_result = self.cache.get(key)
            if get_result != value:
                success = False
            
            # Delete the key
            delete_result = self.cache.delete(key)
            if not delete_result:
                success = False
            
            # Verify it's deleted
            verify_result = self.cache.get(key)
            if verify_result is not None:
                success = False
                
        except Exception as e:
            success = False
            exception = e
        
        # Report the sequence result
        response_time = (time.time() - start_time) * 1000
        events.request.fire(
            request_type="sequence",
            name="mixed_operations",
            response_time=response_time,
            response_length=0,
            exception=exception,
            context={}
        )

# Use this to run the test:
# locust -f locustfile.py
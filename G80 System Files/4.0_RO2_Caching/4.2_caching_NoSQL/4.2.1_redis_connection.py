"""
Redis Cloud Connection Test Script

Purpose: This script tests the connection to Redis Cloud, which will serve as
the in-memory cache layer for the NoSQL database performance comparison research.

Architecture Overview:
    Application → Redis Cloud (Cache Layer) → MongoDB (Primary Database)
    
PREREQUISITES:
1. Set up a Redis Cloud instance 
2. Get your Redis Cloud connection details:
   - Host/Endpoint
   - Port
   - Username (usually "default")
   - Password (created during Redis Cloud setup)
   
This test verifies that:
1. Python can connect to the remote Redis Cloud instance
2. Basic CRUD operations work correctly
3. The cache layer is ready for implementing read-through caching
"""

import redis

REDIS_HOST = ''  # Replace with your Redis Cloud host
REDIS_PORT = 1234                     # Replace with your Redis Cloud port
REDIS_USERNAME = ''                   # Usually "default" for Redis Cloud
REDIS_PASSWORD = ' '                  # Replace with your Redis Cloud password
# ============================================================================

# Initialize Redis connection
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
)

# Test connection
print(f"Ping: {r.ping()}")

# Test set/get
success = r.set('foo', 'bar')
print(f"Set successful: {success}")

result = r.get('foo')
print(f"Get result: {result}")

# Clean up
r.delete('foo')
print("Test completed successfully")
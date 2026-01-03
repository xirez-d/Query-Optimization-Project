# -*- coding: utf-8 -*-
"""
Test MongoDB connection and verify collections exist
"""

import pymongo

# MongoDB connection settings (update with your details)
MONGO_URI = "mongodb://localhost:27017/"  # Default MongoDB local connection
MONGO_DB_NAME = "Olist"  # REPLACE THIS with your actual database name

try:
    # Connect to MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    
    # List all databases
    print("Available databases:")
    for db_name in client.list_database_names():
        print(f"  - {db_name}")
    
    # Connect to your database
    db = client[MONGO_DB_NAME]
    
    # List collections in your database
    print(f"\nCollections in database '{MONGO_DB_NAME}':")
    collections = db.list_collection_names()
    for collection in collections:
        print(f"  - {collection}")
    
    # Check if required collections exist
    required_collections = ['orders', 'customers', 'payments', 'query_cache', 'cache_metrics']
    missing = [col for col in required_collections if col not in collections]
    
    if missing:
        print(f"\nMissing collections: {missing}")
    else:
        print("\nAll required collections exist!")
    
    # Test document count in orders collection
    if 'orders' in collections:
        orders_count = db.orders.count_documents({})
        print(f"Orders collection has {orders_count:,} documents")
    
    client.close()
    
except Exception as e:
    print(f"Connection failed: {e}")
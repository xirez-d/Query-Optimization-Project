"""
NoSQL Cache Implementation 
"""

import redis
import pymongo
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
import hashlib
from bson import json_util

class NoSQLCacheSystem:
    def __init__(self, mongo_uri: str, mongo_db: str, redis_host: str, 
                 redis_port: int, redis_password: str, redis_username: str = "default"):
        """Initialize connections"""
        self.mongo_client = pymongo.MongoClient(mongo_uri)
        self.db = self.mongo_client[mongo_db]
        
        self.orders = self.db.orders
        self.customers = self.db.customers
        self.payments = self.db.payments
        self.query_cache = self.db.query_cache
        self.cache_metrics = self.db.cache_metrics
        
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            decode_responses=True
        )
        
        print("NoSQL Cache System initialized")
    
    def record_metrics(self, test_type: str, cache_status: str, query_type: str,
                       test_case: str, response_time_ms: float, rows_returned: int = 0):
        """Record metrics in MongoDB"""
        metric = {
            "test_type": test_type,
            "cache_status": cache_status,
            "query_type": query_type,
            "test_case": test_case,
            "response_time_ms": round(response_time_ms, 2),
            "rows_returned": rows_returned,
            "run_time": datetime.now()
        }
        self.cache_metrics.insert_one(metric)
        return metric
    
    def get_cached_complex_analysis(self, start_date: str, end_date: str, 
                                    use_cache: bool = True) -> List[Dict]:
        """Main cache function - same as Oracle implementation"""
        start_time = time.time()
        cache_key = f"COMPLEX_{start_date}_TO_{end_date}"
        cache_status = "N/A"
        test_type = "with_cache" if use_cache else "without_cache"
        
        result_data = []
        row_count = 0
        
        try:
            # Convert input dates to string format matching your data
            start_date_str = f"{start_date} 00:00:00"
            end_date_str = f"{end_date} 23:59:59"
            
            if use_cache:
                # Check Redis
                cached_result = self.redis.get(cache_key)
                
                if cached_result:
                    cache_status = "HIT"
                    result_data = json.loads(cached_result)
                    row_count = len(result_data)
                    self.redis.expire(cache_key, 1800)
                    print(f"   Redis Cache HIT for {cache_key}")
                    
                else:
                    # Check MongoDB cache
                    mongo_cache = self.query_cache.find_one({
                        "cache_key": cache_key,
                        "created_time": {"$gt": datetime.now() - timedelta(minutes=30)}
                    })
                    
                    if mongo_cache:
                        cache_status = "HIT"
                        result_data = json.loads(mongo_cache["result_json"])
                        row_count = mongo_cache["rows_returned"]
                        
                        self.redis.setex(
                            cache_key,
                            1800,
                            json.dumps(result_data, default=json_util.default)
                        )
                        
                        self.query_cache.update_one(
                            {"cache_key": cache_key},
                            {
                                "$set": {"last_accessed": datetime.now()},
                                "$inc": {"access_count": 1}
                            }
                        )
                        
                        print(f"   MongoDB Cache HIT for {cache_key}")
                    
                    else:
                        cache_status = "MISS"
                        print(f"   Cache MISS for {cache_key}")
            
            # Execute query if no cache or cache miss
            if not use_cache or cache_status == "MISS":
                # MongoDB Aggregation with STRING date comparison
                pipeline = [
                    {
                        "$match": {
                            "order_status": "delivered",
                            "order_purchase_timestamp": {
                                "$gte": start_date_str,
                                "$lte": end_date_str
                            }
                        }
                    },
                    {
                        "$lookup": {
                            "from": "customers",
                            "localField": "customer_id",
                            "foreignField": "customer_id",
                            "as": "customer_info"
                        }
                    },
                    {"$unwind": {"path": "$customer_info", "preserveNullAndEmptyArrays": True}},
                    {
                        "$lookup": {
                            "from": "payments",
                            "localField": "order_id",
                            "foreignField": "order_id",
                            "as": "payment_info"
                        }
                    },
                    {
                        "$project": {
                            "order_id": 1,
                            "order_status": 1,
                            "order_purchase_timestamp": 1,
                            "customer_state": "$customer_info.customer_state",
                            "payment_count": {"$size": "$payment_info"},
                            "avg_payment": {"$avg": "$payment_info.payment_value"}
                        }
                    },
                    {"$sort": {"order_purchase_timestamp": 1}}
                ]
                
                cursor = self.orders.aggregate(pipeline)
                result_data = list(cursor)
                row_count = len(result_data)
                
                # Store in cache if enabled
                if use_cache and cache_status == "MISS":
                    self.redis.setex(
                        cache_key,
                        1800,
                        json.dumps(result_data, default=json_util.default)
                    )
                    
                    cache_doc = {
                        "cache_key": cache_key,
                        "result_json": json.dumps(result_data, default=json_util.default),
                        "query_text": f"Complex JOIN: delivered orders from {start_date} to {end_date}",
                        "rows_returned": row_count,
                        "created_time": datetime.now(),
                        "last_accessed": datetime.now(),
                        "access_count": 1
                    }
                    
                    self.query_cache.delete_one({"cache_key": cache_key})
                    self.query_cache.insert_one(cache_doc)
                    
                    print(f"   Result cached with {row_count} rows")
            
            # Calculate response time
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000
            
            # Record metrics
            self.record_metrics(
                test_type=test_type,
                cache_status=cache_status,
                query_type="complex_join",
                test_case=f"{start_date}_to_{end_date}",
                response_time_ms=response_time_ms,
                rows_returned=row_count
            )
            
            print(f"   Query completed: {row_count} rows, {response_time_ms:.2f} ms")
            return result_data[:5] if result_data else []
            
        except Exception as e:
            self.record_metrics(
                test_type=test_type,
                cache_status="ERROR",
                query_type="complex_join",
                test_case=f"{start_date}_to_{end_date}",
                response_time_ms=-1
            )
            print(f" Error: {e}")
            raise


def run_complete_tests():
    """Run COMPLETE tests for comparison with Oracle"""
    
    # Configuration
    MONGO_URI = "mongodb://localhost:27017/"
    MONGO_DB = "Olist"
    
    REDIS_HOST = "redis-18745.c267.us-east-1-4.ec2.cloud.redislabs.com"
    REDIS_PORT = 18745
    REDIS_USERNAME = "default"
    REDIS_PASSWORD = "Vwdx5cbcpawpIs5m6QImdHMILpkxbo3y"
    
    # Initialize
    cache_system = NoSQLCacheSystem(
        mongo_uri=MONGO_URI,
        mongo_db=MONGO_DB,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_username=REDIS_USERNAME,
        redis_password=REDIS_PASSWORD
    )
    
    print("\n" + "="*60)
    print("COMPLETE NOSQL CACHE TESTS FOR ORACLE COMPARISON")
    print("="*60)
    
    # ONLY TEST OCTOBER 2017 (SAME AS ORACLE)
    test_date = "2017-10-01"
    test_end_date = "2017-10-31"
    
    print(f"\nTesting October 2017")
    print(f"Date range: {test_date} to {test_end_date}")
    print("-" * 50)
    
    # Test 1: Cache Miss
    print(f"\n1. Cache Miss (first run)")
    results1 = cache_system.get_cached_complex_analysis(
        start_date=test_date,
        end_date=test_end_date,
        use_cache=True
    )
    
    # Test 2: Cache Hit
    print(f"\n2. Cache Hit (second run)")
    results2 = cache_system.get_cached_complex_analysis(
        start_date=test_date,
        end_date=test_end_date,
        use_cache=True
    )
    
    # Test 3: No Cache
    print(f"\n3. No Cache (baseline)")
    results3 = cache_system.get_cached_complex_analysis(
        start_date=test_date,
        end_date=test_end_date,
        use_cache=False
    )
    
    print("\n" + "="*60)
    print("TESTS COMPLETE!")

if __name__ == "__main__":
    run_complete_tests()
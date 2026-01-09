# -*- coding: utf-8 -*-
"""
NoSQL Cache Implementation for Performance Testing
"""

import redis
import pymongo
import time
from datetime import datetime, timedelta, date
from typing import List


class NoSQLCacheSystem:
    def __init__(self, mongo_uri: str, mongo_db: str, redis_host: str,
                 redis_port: int, redis_password: str, redis_username: str = "default"):

        self.mongo_client = pymongo.MongoClient(mongo_uri)
        self.db = self.mongo_client[mongo_db]

        self.orders = self.db.orders
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

    def get_cached_row_count(self, start_date: str, end_date: str, use_cache: bool = True) -> int:
        """
        Read-through caching for row count of delivered orders.
        """
        start_time = time.time()
        cache_key = f"COUNT_{start_date}_TO_{end_date}"
        cache_status = "N/A"
        test_type = "with_cache" if use_cache else "without_cache"

        row_count = 0

        if use_cache:
            # Check Redis cache first
            cached_count = self.redis.get(cache_key)
            if cached_count is not None:
                cache_status = "HIT"
                row_count = int(cached_count)
            else:
                # Check MongoDB cache
                mongo_cache = self.query_cache.find_one({
                    "cache_key": cache_key,
                    "created_time": {"$gt": datetime.now() - timedelta(minutes=30)}
                })
                if mongo_cache:
                    cache_status = "HIT"
                    row_count = mongo_cache["rows_returned"]
                    self.redis.setex(cache_key, 1800, row_count)
                else:
                    cache_status = "MISS"

        if not use_cache or cache_status == "MISS":
            # Run aggregation (count only)
            row_count = self.orders.count_documents({
                "order_status": "delivered",
                "order_purchase_timestamp": {
                    "$gte": f"{start_date} 00:00:00",
                    "$lte": f"{end_date} 23:59:59"
                }
            })

            if use_cache and cache_status == "MISS":
                # Save to Redis & Mongo
                self.redis.setex(cache_key, 1800, row_count)
                self.query_cache.delete_one({"cache_key": cache_key})
                self.query_cache.insert_one({
                    "cache_key": cache_key,
                    "rows_returned": row_count,
                    "created_time": datetime.now(),
                    "last_accessed": datetime.now(),
                    "access_count": 1
                })

        response_time_ms = (time.time() - start_time) * 1000
        self.record_metrics(test_type, cache_status, "row_count", f"{start_date}_to_{end_date}", response_time_ms, row_count)
        return row_count


def run_complete_tests():
    """Run multiple queries to produce meaningful cache hit ratio"""
    MONGO_URI = "mongodb://localhost:27017/"
    MONGO_DB = "Olist"
    REDIS_HOST = "redis-18745.c267.us-east-1-4.ec2.cloud.redislabs.com"
    REDIS_PORT = 18745
    REDIS_USERNAME = "default"
    REDIS_PASSWORD = "Vwdx5cbcpawpIs5m6QImdHMILpkxbo3y"

    cache_system = NoSQLCacheSystem(MONGO_URI, MONGO_DB, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_USERNAME)

    # Clear cache & metrics
    cache_system.redis.flushdb()
    cache_system.query_cache.delete_many({})
    cache_system.cache_metrics.delete_many({})
    print("Cache and metrics cleared")

    # --- PARAMETERS ---
    start_date = date(2017, 10, 1)
    total_days = 10          # number of different date queries
    repeats_per_day = 10     # repeat each date query to generate cache hits
    no_cache_runs = 5        # number of runs without cache

    # --- 1. Warm-up / repeated cached queries ---
    for i in range(total_days):
        current_date = start_date + timedelta(days=i)
        start_str = current_date.strftime("%Y-%m-%d")
        end_str = start_str
        for _ in range(repeats_per_day):
            cache_system.get_cached_row_count(start_str, end_str, use_cache=True)

    # --- 2. No-cache baseline ---
    for i in range(total_days):
        current_date = start_date + timedelta(days=i)
        start_str = current_date.strftime("%Y-%m-%d")
        end_str = start_str
        for _ in range(no_cache_runs):
            cache_system.get_cached_row_count(start_str, end_str, use_cache=False)

    print("\nTESTS COMPLETE!")


def show_nosql_cache_results():
    """Aggregate and display performance metrics"""
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client.Olist

    metrics = list(db.cache_metrics.find({"query_type": "row_count"}))
    if not metrics:
        print("No metrics found.")
        return

    total_queries = len(metrics)
    cache_hits = sum(1 for m in metrics if m["cache_status"] == "HIT")
    cache_misses = sum(1 for m in metrics if m["cache_status"] == "MISS")
    no_cache_count = sum(1 for m in metrics if m["test_type"] == "without_cache")

    # Response times
    hit_times = [m["response_time_ms"] for m in metrics if m["cache_status"] == "HIT"]
    miss_times = [m["response_time_ms"] for m in metrics if m["cache_status"] == "MISS"]
    no_cache_times = [m["response_time_ms"] for m in metrics if m["test_type"] == "without_cache"]

    avg_hit = sum(hit_times)/len(hit_times) if hit_times else 0
    avg_miss = sum(miss_times)/len(miss_times) if miss_times else 0
    avg_no_cache = sum(no_cache_times)/len(no_cache_times) if no_cache_times else 0
    cache_hit_ratio = (cache_hits / (cache_hits + cache_misses) * 100) if (cache_hits + cache_misses) > 0 else 0

    print("\n" + "="*90)
    print("NOSQL CACHE PERFORMANCE RESULTS")
    print("="*90)
    print(f"{'Metric':<35} {'Value'}")
    print("-"*90)
    print(f"{'Total Queries':<35} {total_queries}")
    print(f"{'Cache Hits':<35} {cache_hits}")
    print(f"{'Cache Misses':<35} {cache_misses}")
    print(f"{'No-cache Runs':<35} {no_cache_count}")
    print(f"{'Cache Hit Ratio (%)':<35} {cache_hit_ratio:.2f}")
    print(f"{'Avg Response Time (HIT) ms':<35} {avg_hit:.2f}")
    print(f"{'Avg Response Time (MISS) ms':<35} {avg_miss:.2f}")
    print(f"{'Avg Response Time (No Cache) ms':<35} {avg_no_cache:.2f}")

    print("\nDetailed Breakdown (HIT vs MISS vs WITHOUT CACHE):")
    print(f"{'Cache Status':<20} {'Avg Response Time (ms)'}")
    print("-"*50)
    if hit_times:
        print(f"{'HIT':<20} {avg_hit:.2f}")
    if miss_times:
        print(f"{'MISS':<20} {avg_miss:.2f}")
    if no_cache_times:
        print(f"{'WITHOUT CACHE':<20} {avg_no_cache:.2f}")

    client.close()


if __name__ == "__main__":
    run_complete_tests()
    show_nosql_cache_results()

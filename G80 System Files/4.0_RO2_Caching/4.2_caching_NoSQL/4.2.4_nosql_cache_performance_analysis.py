"""
NoSQL Cache Performance Comparison
"""

import pymongo
from datetime import datetime

def calculate_nosql_performance():
    """Calculate metrics"""
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client.Olist
    
    print("="*80)
    print("NOSQL CACHE PERFORMANCE SUMMARY ")
    print("="*80)
    
    # Get all complex join metrics
    metrics = list(db.cache_metrics.find({
        "query_type": "complex_join"
    }))
    
    if not metrics:
        print("No metrics found. Run tests first.")
        return
    
    # Calculate statistics 
    total_queries = len(metrics)
    cache_hits = sum(1 for m in metrics if m.get("cache_status") == "HIT")
    cache_misses = sum(1 for m in metrics if m.get("cache_status") == "MISS")
    
    # Calculate averages (SAME as Oracle)
    with_cache_times = [m["response_time_ms"] for m in metrics 
                       if m.get("test_type") == "with_cache" and m["response_time_ms"] > 0]
    without_cache_times = [m["response_time_ms"] for m in metrics 
                          if m.get("test_type") == "without_cache" and m["response_time_ms"] > 0]
    
    avg_with_cache = sum(with_cache_times) / len(with_cache_times) if with_cache_times else 0
    avg_without_cache = sum(without_cache_times) / len(without_cache_times) if without_cache_times else 0
    
    # Calculate percentages (SAME as Oracle)
    cache_hit_ratio = (cache_hits / total_queries * 100) if total_queries > 0 else 0
    improvement = ((avg_without_cache - avg_with_cache) / avg_without_cache * 100) if avg_without_cache > 0 else 0
    
    # Print results in SAME format as Oracle
    print(f"\n{'QUERY CATEGORY':<20} {'TOTAL QUERIES':<15} {'CACHE HITS':<12} {'CACHE MISSES':<14} {'HIT RATIO %':<12} {'AVG WITH CACHE':<16} {'AVG WITHOUT CACHE':<18} {'IMPROVEMENT %'}")
    print("-" * 120)
    
    print(f"{'COMPLEX QUERIES':<20} "
          f"{total_queries:<15} "
          f"{cache_hits:<12} "
          f"{cache_misses:<14} "
          f"{cache_hit_ratio:<12.2f} "
          f"{avg_with_cache:<16.2f} "
          f"{avg_without_cache:<18.2f} "
          f"{improvement:<12.2f}")
    
    # Detailed breakdown by test case
    print(f"\n\n{'='*80}")
    print("DETAILED TEST CASE ANALYSIS")
    print("="*80)
    
    # Group by test case
    test_cases = {}
    for metric in metrics:
        test_case = metric.get("test_case", "unknown")
        if test_case not in test_cases:
            test_cases[test_case] = []
        test_cases[test_case].append(metric)
    
    print(f"\n{'TEST CASE':<30} {'ROWS':<8} {'CACHE MISS':<12} {'CACHE HIT':<12} {'NO CACHE':<12} {'IMPROVEMENT %':<15}")
    print("-" * 90)
    
    for test_case, case_metrics in test_cases.items():
        if case_metrics:
            rows = case_metrics[0].get("rows_returned", 0)
            
            # Find best times
            miss_times = [m["response_time_ms"] for m in case_metrics 
                         if m.get("cache_status") == "MISS"]
            hit_times = [m["response_time_ms"] for m in case_metrics 
                        if m.get("cache_status") == "HIT"]
            no_cache_times = [m["response_time_ms"] for m in case_metrics 
                             if m.get("test_type") == "without_cache"]
            
            best_miss = min(miss_times) if miss_times else 0
            best_hit = min(hit_times) if hit_times else 0
            best_no_cache = min(no_cache_times) if no_cache_times else 0
            
            improvement_case = ((best_no_cache - best_hit) / best_no_cache * 100) if best_no_cache > 0 else 0
            
            print(f"{test_case:<30} "
                  f"{rows:<8} "
                  f"{best_miss:<12.2f} "
                  f"{best_hit:<12.2f} "
                  f"{best_no_cache:<12.2f} "
                  f"{improvement_case:<15.2f}")
    
    
    client.close()


if __name__ == "__main__":
    calculate_nosql_performance()

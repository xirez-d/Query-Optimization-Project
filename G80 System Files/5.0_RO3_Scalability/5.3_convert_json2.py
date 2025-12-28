# -*- coding: utf-8 -*-
"""
CONVERTING 10% SAMPLED DATA INTO JSON SCRIPT
=============================================
Purpose: Convert the second 10% csv data into json files for RO3 implementation in MongoDB
NOTE: After converting json files, load all JSON files into MongoDB
"""


import pandas as pd
import json
import os
from pathlib import Path

# =========================================================
# PORTABLE FILE PATH CONFIGURATION
# =========================================================

# Get the directory where THIS Python script is located (5.0_RO3_Scalability/)
SCRIPT_DIR = Path(__file__).parent

FIRST_10PERCENT_JSON_DIR = SCRIPT_DIR.parent / "1.0_Datasets" / "sampled_data_json"   # First 10% JSON files
SECOND_10PERCENT_CSV_DIR = SCRIPT_DIR.parent / "1.0_Datasets" / "sampled_data_csv2"   # Second 10% CSV folder
JSON_OUTPUT_DIR = SCRIPT_DIR.parent / "1.0_Datasets" / "sampled_data_json2"           # Output for second 10% JSON

# =========================================================
# LOAD FIRST 10% IDs TO PREVENT OVERLAP
# =========================================================

def load_first_10percent_ids():
    """Load all IDs from first 10% data to prevent overlap"""
    
    print("Loading first 10% IDs to prevent overlap...")
    
    first10_ids = {
        'customers': set(),
        'orders': set(),
        'products': set(),
        'order_items': set(),
        'reviews': set(),
        'payments': set()
    }
    
    try:
        # Load customers
        customers_path = os.path.join(FIRST_10PERCENT_JSON_DIR, "olist_customers_dataset.json")
        if os.path.exists(customers_path):
            with open(customers_path, 'r', encoding='utf-8') as f:
                customers = json.load(f)
                first10_ids['customers'] = {c['customer_id'] for c in customers}
            print(f"  • Loaded {len(first10_ids['customers'])} customer IDs from first 10%")
        
        # Load orders
        orders_path = os.path.join(FIRST_10PERCENT_JSON_DIR, "olist_orders_dataset.json")
        if os.path.exists(orders_path):
            with open(orders_path, 'r', encoding='utf-8') as f:
                orders = json.load(f)
                first10_ids['orders'] = {o['order_id'] for o in orders}
            print(f"  • Loaded {len(first10_ids['orders'])} order IDs from first 10%")
        
        # Load products
        products_path = os.path.join(FIRST_10PERCENT_JSON_DIR, "olist_products_dataset.json")
        if os.path.exists(products_path):
            with open(products_path, 'r', encoding='utf-8') as f:
                products = json.load(f)
                first10_ids['products'] = {p['product_id'] for p in products}
            print(f"  • Loaded {len(first10_ids['products'])} product IDs from first 10%")
            
    except Exception as e:
        print(f"Warning: Could not load first 10% IDs: {e}")
        # Return empty sets if loading fails
        return first10_ids
    
    return first10_ids

# =========================================================
# CONVERT SECOND 10% WITH OVERLAP CHECK
# =========================================================

def convert_second_10percent_no_overlap():
    """Convert second 10% CSV to JSON, ensuring NO overlap with first 10%"""
    
    print("=" * 60)
    print("SECOND 10% CSV TO JSON CONVERTER (NO OVERLAP)")
    print("=" * 60)
    
    # Load first 10% IDs
    first10_ids = load_first_10percent_ids()
    
    # Create output directory
    Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Mapping of CSV files to collections and ID columns
    datasets_config = [
        {
            'csv_file': 'olist_customers_dataset_sample.csv',
            'json_file': 'olist_customers_dataset_second10percent.json',
            'collection': 'customers',
            'id_column': 'customer_id',
            'exclude_ids': first10_ids['customers']
        },
        {
            'csv_file': 'olist_orders_dataset_sample.csv',
            'json_file': 'olist_orders_dataset_second10percent.json',
            'collection': 'orders',
            'id_column': 'order_id',
            'exclude_ids': first10_ids['orders']
        },
        {
            'csv_file': 'olist_products_dataset_sample.csv',
            'json_file': 'olist_products_dataset_second10percent.json',
            'collection': 'products',
            'id_column': 'product_id',
            'exclude_ids': first10_ids['products']
        },
        {
            'csv_file': 'olist_order_items_dataset_sample.csv',
            'json_file': 'olist_order_items_dataset_second10percent.json',
            'collection': 'order_items',
            'id_column': None,  # Composite key, handled differently
            'exclude_ids': set()
        },
        {
            'csv_file': 'olist_order_payments_dataset_sample.csv',
            'json_file': 'olist_order_payments_dataset_second10percent.json',
            'collection': 'payments',
            'id_column': None,  # Composite key
            'exclude_ids': set()
        },
        {
            'csv_file': 'olist_order_reviews_dataset_sample.csv',
            'json_file': 'olist_order_reviews_dataset_second10percent.json',
            'collection': 'reviews',
            'id_column': 'review_id',
            'exclude_ids': set()  # No first 10% reviews loaded
        },
        {
            'csv_file': 'olist_geolocation_dataset_sample.csv',
            'json_file': 'olist_geolocation_dataset_second10percent.json',
            'collection': 'geolocation',
            'id_column': 'geolocation_id',
            'exclude_ids': set()  # No first 10% geolocation loaded
        }
    ]
    
    converted_files = 0
    overlap_found = False
    
    print("\n" + "=" * 60)
    print("PROCESSING SECOND 10% DATA")
    print("=" * 60)
    
    for config in datasets_config:
        csv_file = config['csv_file']
        input_path = os.path.join(SECOND_10PERCENT_CSV_DIR, csv_file)
        
        if not os.path.exists(input_path):
            print(f"File not found: {csv_file}")
            continue
        
        try:
            # Read CSV
            df = pd.read_csv(input_path, low_memory=False)
            original_count = len(df)
            
            # Remove rows that overlap with first 10%
            if config['id_column'] and config['exclude_ids']:
                before_filter = len(df)
                df = df[~df[config['id_column']].isin(config['exclude_ids'])]
                after_filter = len(df)
                
                if before_filter > after_filter:
                    overlap_count = before_filter - after_filter
                    print(f"Removed {overlap_count} overlapping {config['collection']} from second 10%")
                    overlap_found = True
            
            # Special handling for order_items (composite key)
            if config['collection'] == 'order_items':
                # Remove items for orders that are in first 10%
                if first10_ids['orders']:
                    before_filter = len(df)
                    df = df[~df['order_id'].isin(first10_ids['orders'])]
                    after_filter = len(df)
                    if before_filter > after_filter:
                        overlap_count = before_filter - after_filter
                        print(f" Removed {overlap_count} order_items for overlapping orders")
                        overlap_found = True
            
            # Save filtered data
            if len(df) > 0:
                output_path = os.path.join(JSON_OUTPUT_DIR, config['json_file'])
                df.to_json(output_path, orient='records', indent=2)
                
                print(f" {csv_file} → {config['json_file']}")
                print(f"  Original: {original_count:,} rows, Filtered: {len(df):,} rows")
                converted_files += 1
            else:
                print(f"{csv_file} → No data after removing overlaps")
                
        except Exception as e:
            print(f"✗ Error processing {csv_file}: {e}")
    
    # =========================================================
    # VERIFY NO OVERLAP
    # =========================================================
    print("\n" + "=" * 60)
    print("OVERLAP VERIFICATION")
    print("=" * 60)
    
    if overlap_found:
        print("⚠ Overlap detected and removed from second 10% data")
    else:
        print("No overlap found between first and second 10% data")
    
    # Double-check by loading and comparing
    try:
        # Check customers
        second_customers_path = os.path.join(JSON_OUTPUT_DIR, "olist_customers_dataset_second10percent.json")
        if os.path.exists(second_customers_path):
            with open(second_customers_path, 'r', encoding='utf-8') as f:
                second_customers = json.load(f)
                second_customer_ids = {c['customer_id'] for c in second_customers}
                
            overlap = second_customer_ids.intersection(first10_ids['customers'])
            if overlap:
                print(f" ERROR: Still have {len(overlap)} overlapping customer IDs")
            else:
                print(f"Customers: {len(second_customer_ids):,} unique IDs, 0 overlap")
    
    except Exception as e:
        print(f"Note: Could not verify: {e}")
    
    # =========================================================
    # SUMMARY
    # =========================================================
    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Successfully converted: {converted_files}/{len(datasets_config)} files")
    print(f"Output folder: {JSON_OUTPUT_DIR}")
    
    if converted_files > 0:
        print("\nGenerated JSON files (Second 10% - No Overlap):")
        for file in sorted(os.listdir(JSON_OUTPUT_DIR)):
            if file.endswith('.json'):
                filepath = os.path.join(JSON_OUTPUT_DIR, file)
                size_kb = os.path.getsize(filepath) / 1024
                with open(filepath, 'r', encoding='utf-8') as f:
                    row_count = len(json.load(f))
                print(f"  • {file} ({row_count:,} rows, {size_kb:.1f} KB)")
    
    print("\n" + "=" * 60)
    print("MONGODB IMPORT (Safe - No Overlap)")
    print("=" * 60)
    print(f"cd \"{SCRIPT_DIR}\"")
    print("\n# Import commands (safe to append - no duplicates):")
    for config in datasets_config:
        if os.path.exists(os.path.join(JSON_OUTPUT_DIR, config['json_file'])):
            print(f"mongoimport --db ecommerce --collection {config['collection']} \\")
            print(f"  --file ./json_sampled_data2/{config['json_file']} --jsonArray")
    print("=" * 60)

# =========================================================
# MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    # Check if folders exist
    if not os.path.exists(FIRST_10PERCENT_JSON_DIR):
        print(f"ERROR: First 10% JSON folder not found: {FIRST_10PERCENT_JSON_DIR}")
        print("Please run your first 10% conversion script first.")
    elif not os.path.exists(SECOND_10PERCENT_CSV_DIR):
        print(f"ERROR: Second 10% CSV folder not found: {SECOND_10PERCENT_CSV_DIR}")
    else:
        convert_second_10percent_no_overlap()
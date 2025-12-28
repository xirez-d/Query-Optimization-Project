# This python script converts the sampled .csv files into json files

import pandas as pd
import json
import os
from pathlib import Path
import sys  # Added for sys.exit()

# =========================================================
# PORTABLE FILE PATH CONFIGURATION
# =========================================================

# Get the directory where THIS Python script is located
SCRIPT_DIR = Path(__file__).parent

# INPUT: Sampled CSV files are in the 'sampled_data_csv' folder (in 1.0_Datasets)
SAMPLED_INPUT_DIR = SCRIPT_DIR.parent / "1.0_Datasets" / "sampled_data_csv"

# OUTPUT: JSON files will be saved in 'sampled_data_json' folder (in 1.0_Datasets)
JSON_OUTPUT_DIR = SCRIPT_DIR.parent / "1.0_Datasets" / "sampled_data_json"

# All sampled datasets (from your sampling script)
SAMPLED_DATASETS = [
    "olist_customers_dataset_sample.csv",
    "olist_orders_dataset_sample.csv",
    "olist_order_items_dataset_sample.csv", 
    "olist_order_payments_dataset_sample.csv",
    "olist_order_reviews_dataset_sample.csv",
    "olist_products_dataset_sample.csv",
    "olist_sellers_dataset.csv",  # Keep original 
    "olist_geolocation_dataset_sample.csv",
    "product_category_name_translation.csv"  # Keep original 
]

# =========================================================
# CONVERSION FUNCTION
# =========================================================

def convert_sampled_csv_to_json():
    """Convert sampled CSV files to JSON files"""
    
    print("=" * 60)
    print("SAMPLED CSV TO JSON CONVERTER")
    print("=" * 60)
    print(f"Script location: {SCRIPT_DIR}")
    print(f"Sampled CSV folder: {SAMPLED_INPUT_DIR}")
    print(f"JSON output folder: {JSON_OUTPUT_DIR}")
    print("=" * 60)
    
    # Check if input directory exists
    if not SAMPLED_INPUT_DIR.exists():
        print(f"ERROR: 'sampled_data_csv' folder not found at: {SAMPLED_INPUT_DIR}")
        print("Please run your sampling script first to create the sampled CSV files.")
        print("Expected location: G80 System Files/1.0_Datasets/sampled_data_csv/")
        return
    
    # Create output directory if it doesn't exist
    JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    converted_files = 0
    missing_files = []
    
    for csv_file in SAMPLED_DATASETS:
        input_path = SAMPLED_INPUT_DIR / csv_file
        
        # Use original filenames for JSON (remove '_sample' from CSV filenames)
        # This keeps consistency with your SQL setup
        if csv_file.endswith('_sample.csv'):
            json_filename = csv_file.replace('_sample.csv', '.json')
        else:
            json_filename = csv_file.replace('.csv', '.json')
        
        output_path = JSON_OUTPUT_DIR / json_filename
        
        try:
            if input_path.exists():
                # Read CSV (exactly as-is)
                df = pd.read_csv(input_path, low_memory=False)
                
                # Save as JSON (preserving all structure)
                df.to_json(str(output_path), orient='records', indent=2)
                
                print(f"{csv_file} → {json_filename} ({len(df):,} rows)")
                converted_files += 1
            else:
                # For sellers and translation - check if in olist_datasets folder
                if csv_file in ["olist_sellers_dataset.csv", "product_category_name_translation.csv"]:
                    # Try original olist_datasets folder
                    original_input_path = SCRIPT_DIR.parent / "1.0_Datasets" / "olist_datasets" / csv_file
                    if original_input_path.exists():
                        df = pd.read_csv(str(original_input_path), low_memory=False)
                        df.to_json(str(output_path), orient='records', indent=2)
                        print(f"{csv_file} (original from olist_datasets) → {json_filename} ({len(df):,} rows)")
                        converted_files += 1
                    else:
                        print(f"File not found: {csv_file} (not in sampled_data_csv or olist_datasets)")
                        missing_files.append(csv_file)
                else:
                    print(f"File not found in sampled_data_csv: {csv_file}")
                    missing_files.append(csv_file)
                    
        except Exception as e:
            print(f"Error converting {csv_file}: {str(e)[:100]}...")
    
    # =========================================================
    # VERIFY RELATIONSHIPS ARE PRESERVED
    # =========================================================
    print("\n" + "=" * 60)
    print("DATA RELATIONSHIP VERIFICATION")
    print("=" * 60)
    
    # Load key datasets to verify relationships
    try:
        # Load customers JSON
        customers_path = JSON_OUTPUT_DIR / "olist_customers_dataset.json"
        if customers_path.exists():
            with open(customers_path, 'r', encoding='utf-8') as f:
                customers_data = json.load(f)
            
            # Load orders JSON
            orders_path = JSON_OUTPUT_DIR / "olist_orders_dataset.json"
            if orders_path.exists():
                with open(orders_path, 'r', encoding='utf-8') as f:
                    orders_data = json.load(f)
                
                # Count orders per customer
                customer_ids_in_orders = set(order['customer_id'] for order in orders_data)
                customer_ids_in_customers = set(customer['customer_id'] for customer in customers_data)
                
                print(f"Customers in dataset: {len(customer_ids_in_customers):,}")
                print(f"Customers with orders: {len(customer_ids_in_orders):,}")
                print(f"All orders belong to sampled customers: {customer_ids_in_orders.issubset(customer_ids_in_customers)}")
        
        # Count sample sizes
        print("\nDataset sizes (sampled):")
        json_files = list(JSON_OUTPUT_DIR.glob("*.json"))
        for json_path in sorted(json_files):
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"  • {json_path.name}: {len(data):,} records")
                
    except Exception as e:
        print(f"Note: Could not verify relationships: {str(e)[:100]}...")
    
    # =========================================================
    # SUMMARY
    # =========================================================
    print("=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    print(f"Successfully converted: {converted_files}/{len(SAMPLED_DATASETS)} files")
    
    if missing_files:
        print(f"\nMissing files:")
        for file in missing_files:
            print(f"  - {file}")
        print(f"\nNote: 'sellers' and 'translation' will look in olist_datasets folder if not in sampled_data_csv")
    
    print(f"\nJSON files saved in: {JSON_OUTPUT_DIR}")
    
    # Show what was created
    if converted_files > 0:
        print(f"\nGenerated JSON files:")
        json_files = list(JSON_OUTPUT_DIR.glob("*.json"))
        for json_path in sorted(json_files):
            size_kb = json_path.stat().st_size / 1024
            print(f"  • {json_path.name} ({size_kb:.1f} KB)")
    
    print("=" * 60)
    
    # Show import commands for MongoDB
    print("\n" + "=" * 60)
    print("MONGODB IMPORT COMMANDS (for sampled data)")
    print("=" * 60)
    print("To import to MongoDB, run these commands in terminal:")
    print(f"cd \"{SCRIPT_DIR.parent}\"")
    print("\nFor each collection:")
    for csv_file in SAMPLED_DATASETS:
        if csv_file.endswith('_sample.csv'):
            json_file = csv_file.replace('_sample.csv', '.json')
            collection_name = csv_file.replace('_sample.csv', '').replace('olist_', '').replace('_dataset', '')
        else:
            json_file = csv_file.replace('.csv', '.json')
            collection_name = csv_file.replace('.csv', '').replace('olist_', '').replace('_dataset', '')
        
        if collection_name == 'product_category_name_translation':
            collection_name = 'category_translation'
        
        # Correct path for MongoDB import
        json_relative_path = f"1.0_Datasets/sampled_data_json/{json_file}"
        print(f"mongoimport --db olist_research --collection {collection_name} \\")
        print(f"            --file {json_relative_path} --jsonArray")
        print()

# =========================================================
# MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    convert_sampled_csv_to_json()
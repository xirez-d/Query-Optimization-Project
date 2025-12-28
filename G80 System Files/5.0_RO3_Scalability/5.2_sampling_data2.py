# -*- coding: utf-8 -*-
"""
SECOND 10% SAMPLING SCRIPT
=============================================
Purpose: Sample a second 10% of data without overlapping with the first 10%
sampled data. This is for RO3 scalability testing.
"""

import pandas as pd
from pathlib import Path
import sys  # For sys.exit()
# ---------- CONFIG ----------
RANDOM_STATE = 42    # Same random seed for reproducibility
SAMPLE_FRAC = 0.10   # Sample 10% of the remaining data

# ---------- PATH CONFIGURATION ----------
# Get the directory where THIS Python script is located (5.0_RO3_Scalability)
script_dir = Path(__file__).parent

# Navigate to the correct folders
# Original data folder
input_folder = script_dir.parent / "1.0_Datasets" / "olist_datasets"

# First 10% samples (already created)
existing_sample_folder = script_dir.parent / "1.0_Datasets" / "sampled_data_csv"

# Second 10% output folder
new_sample_folder = script_dir.parent / "1.0_Datasets" / "sampled_data_csv2"

# Create output folder if it doesn't exist
new_sample_folder.mkdir(parents=True, exist_ok=True)

# ---------- VERIFY FOLDERS EXIST ----------
print(f"Script location: {script_dir}")
print(f"Input folder (original data): {input_folder}")
print(f"Existing samples folder (first 10%): {existing_sample_folder}")
print(f"New samples folder (second 10%): {new_sample_folder}")

# Check if input folder exists
if not input_folder.exists():
    print(f"ERROR: Input folder not found at {input_folder}")
    print("Please make sure the 'olist_datasets' folder exists in 1.0_Datasets/")
    sys.exit(1)

# Check if first 10% samples exist
if not existing_sample_folder.exists():
    print(f"ERROR: First 10% samples folder not found at {existing_sample_folder}")
    print("Please run your first sampling script (2.1_sampling_data_csv.py) first.")
    sys.exit(1)


# ---------- LOAD EXISTING SAMPLED IDs ----------
print("Loading existing sampled IDs...")

# 1. Load existing sampled customer IDs
existing_customers = pd.read_csv(existing_sample_folder / "olist_customers_dataset_sample.csv")
existing_customer_ids = set(existing_customers["customer_id"])

# 2. Load existing sampled order IDs  
existing_orders = pd.read_csv(existing_sample_folder / "olist_orders_dataset_sample.csv")
existing_order_ids = set(existing_orders["order_id"])

# 3. Load existing sampled product IDs
existing_order_items = pd.read_csv(existing_sample_folder / "olist_order_items_dataset_sample.csv")
existing_product_ids = set(existing_order_items["product_id"])

print(f"Existing sampled: {len(existing_customer_ids)} customers, {len(existing_order_ids)} orders")

# ---------- 1. Sample NEW Customers (exclude existing) ----------
all_customers = pd.read_csv(input_folder / "olist_customers_dataset.csv")
# Exclude already sampled customers
remaining_customers = all_customers[~all_customers["customer_id"].isin(existing_customer_ids)]
print(f"Remaining customers to sample from: {len(remaining_customers)}")

# Sample 10% from REMAINING customers
new_customers_sample = remaining_customers.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE)
new_customers_sample.to_csv(new_sample_folder / "olist_customers_dataset_sample.csv", index=False)

# New sampled customer_ids
new_customer_ids = set(new_customers_sample["customer_id"])
print(f"New sampled customers: {len(new_customer_ids)}")

# ---------- 2. Sample NEW Orders (for new customers only) ----------
all_orders = pd.read_csv(input_folder / "olist_orders_dataset.csv")
# Get orders for new customers (and exclude any existing order_ids just in case)
new_orders = all_orders[
    (all_orders["customer_id"].isin(new_customer_ids)) & 
    (~all_orders["order_id"].isin(existing_order_ids))
]
new_orders.to_csv(new_sample_folder / "olist_orders_dataset_sample.csv", index=False)

new_order_ids = set(new_orders["order_id"])
print(f"New sampled orders: {len(new_order_ids)}")

# ---------- 3. Sample NEW Order Items ----------
all_order_items = pd.read_csv(input_folder / "olist_order_items_dataset.csv")
# Keep only items for new orders
new_order_items = all_order_items[all_order_items["order_id"].isin(new_order_ids)]
new_order_items.to_csv(new_sample_folder / "olist_order_items_dataset_sample.csv", index=False)

# New sampled product_ids
new_product_ids = set(new_order_items["product_id"])

# ---------- 4. Sample NEW Products ----------
all_products = pd.read_csv(input_folder / "olist_products_dataset.csv")
# Keep only new products (not in existing sampled products)
new_products = all_products[
    (all_products["product_id"].isin(new_product_ids)) & 
    (~all_products["product_id"].isin(existing_product_ids))
]
new_products.to_csv(new_sample_folder / "olist_products_dataset_sample.csv", index=False)

# ---------- 5. Sample NEW Order Payments ----------
all_order_payments = pd.read_csv(input_folder / "olist_order_payments_dataset.csv")
new_order_payments = all_order_payments[all_order_payments["order_id"].isin(new_order_ids)]
new_order_payments.to_csv(new_sample_folder / "olist_order_payments_dataset_sample.csv", index=False)

# ---------- 6. Sample NEW Order Reviews ----------
all_order_reviews = pd.read_csv(input_folder / "olist_order_reviews_dataset.csv")
# Keep reviews for new orders
new_order_reviews = all_order_reviews[all_order_reviews["order_id"].isin(new_order_ids)]

# Remove duplicates
new_order_reviews = new_order_reviews.drop_duplicates(subset=["review_id"], keep="first")
new_order_reviews.to_csv(new_sample_folder / "olist_order_reviews_dataset_sample.csv", index=False)

# ---------- 7. Sample NEW Geolocation ----------
all_geolocation = pd.read_csv(input_folder / "olist_geolocation_dataset.csv")
# Sample 10% from remaining geolocation
geolocation_sample = all_geolocation.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE)
# Add geolocation_id as primary key
geolocation_sample.insert(0, "geolocation_id", range(1, len(geolocation_sample) + 1))
geolocation_sample.to_csv(new_sample_folder / "olist_geolocation_dataset_sample.csv", index=False)

print("\nNEW 10% sampled datasets saved successfully!")
print(f"Location: {new_sample_folder}")
print(f"New customers: {len(new_customer_ids)}")
print(f"New orders: {len(new_order_ids)}")
print(f"New products: {len(new_product_ids)}")

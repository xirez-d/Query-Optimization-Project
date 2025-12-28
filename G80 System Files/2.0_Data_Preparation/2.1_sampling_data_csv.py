# This python script sample 10% of the original data (except seller and translation datasets)

import pandas as pd
from pathlib import Path
import sys  # Import sys for sys.exit()

# ---------- CONFIG ----------
RANDOM_STATE = 42
SAMPLE_FRAC = 0.10

# Get the current script's directory
script_dir = Path(__file__).parent

# Define paths relative to your new folder structure
# Input: olist_datasets folder (in 1.0_Datasets)
# Output: sampled_data_csv folder (in 1.0_Datasets)
input_folder = script_dir.parent / "1.0_Datasets" / "olist_datasets"
output_folder = script_dir.parent / "1.0_Datasets" / "sampled_data_csv"

# Create output folder if it doesn't exist
output_folder.mkdir(parents=True, exist_ok=True)

print(f"Input folder: {input_folder}")
print(f"Output folder: {output_folder}")

# Check if input folder exists
if not input_folder.exists():
    print(f"ERROR: Input folder not found at {input_folder}")
    print("Please make sure the 'olist_datasets' folder exists in:")
    print("  G80 System Files/1.0_Datasets/olist_datasets/")
    sys.exit(1)  # Changed from exit(1) to sys.exit(1)

# Check if there are files in the input folder
input_files = list(input_folder.glob("*.csv"))
if not input_files:
    print(f"ERROR: No CSV files found in {input_folder}")
    sys.exit(1)  # Changed from exit(1) to sys.exit(1)

# ---------- 1. Sample Customers ----------
print("Sampling customers...")
customers = pd.read_csv(input_folder / "olist_customers_dataset.csv")
customers_sample = customers.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE)
customers_sample.to_csv(output_folder / "olist_customers_dataset_sample.csv", index=False)

# List of sampled customer_ids
sampled_customer_ids = set(customers_sample["customer_id"])

# ---------- 2. Sample Orders ----------
print("Sampling orders...")
orders = pd.read_csv(input_folder / "olist_orders_dataset.csv")
# Keep only orders for sampled customers
orders_sample = orders[orders["customer_id"].isin(sampled_customer_ids)]
orders_sample.to_csv(output_folder / "olist_orders_dataset_sample.csv", index=False)

# List of sampled order_ids
sampled_order_ids = set(orders_sample["order_id"])

# ---------- 3. Sample Order Items ----------
print("Sampling order items...")
order_items = pd.read_csv(input_folder / "olist_order_items_dataset.csv")
# Keep only items for sampled orders
order_items_sample = order_items[order_items["order_id"].isin(sampled_order_ids)]
order_items_sample.to_csv(output_folder / "olist_order_items_dataset_sample.csv", index=False)

# List of sampled product_ids
sampled_product_ids = set(order_items_sample["product_id"])

# ---------- 4. Sample Products ----------
print("Sampling products...")
products = pd.read_csv(input_folder / "olist_products_dataset.csv")
# Keep only products referenced in sampled order items
products_sample = products[products["product_id"].isin(sampled_product_ids)]
products_sample.to_csv(output_folder / "olist_products_dataset_sample.csv", index=False)

# ---------- 5. Sample Order Payments ----------
print("Sampling order payments...")
order_payments = pd.read_csv(input_folder / "olist_order_payments_dataset.csv")
# Keep only payments for sampled orders
order_payments_sample = order_payments[order_payments["order_id"].isin(sampled_order_ids)]
order_payments_sample.to_csv(output_folder / "olist_order_payments_dataset_sample.csv", index=False)

# ---------- 6. Sample Order Reviews ----------
print("Sampling order reviews...")
order_reviews = pd.read_csv(input_folder / "olist_order_reviews_dataset.csv")
# Keep only reviews for sampled orders
order_reviews_sample = order_reviews[order_reviews["order_id"].isin(sampled_order_ids)]

# Remove duplicates based on review_id, keep the first occurrence
order_reviews_sample = order_reviews_sample.drop_duplicates(subset=["review_id"], keep="first")
# Save sampled and cleaned reviews
order_reviews_sample.to_csv(output_folder / "olist_order_reviews_dataset_sample.csv", index=False)

# ---------- 7. Sample Geolocation ----------
print("Sampling geolocation...")
geolocation = pd.read_csv(input_folder / "olist_geolocation_dataset.csv")
geolocation_sample = geolocation.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE).reset_index(drop=True)
# Add geolocation_id as primary key
geolocation_sample.insert(0, "geolocation_id", range(1, len(geolocation_sample) + 1))
geolocation_sample.to_csv(output_folder / "olist_geolocation_dataset_sample.csv", index=False)

print("\nAll sampled datasets saved successfully!")
print(f"Files saved to: {output_folder}")


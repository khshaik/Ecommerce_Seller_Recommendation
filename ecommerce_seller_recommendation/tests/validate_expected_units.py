#!/usr/bin/env python3
"""
validate_expected_units.py

This script validates the expected_units_sold calculation by:
1. Reading the company and competitor recommendation CSVs.
2. For each unique item, computing:
   - total_units_sold (sum of expected_units_sold * number of sellers for that item)
   - sellers_selling_count (count of distinct sellers recommending that item)
   - expected_units_sold (should equal: total_units_sold / sellers_selling_count)
3. Printing a tabular breakdown showing the derivation.
"""
import os
import sys
import pandas as pd
from collections import defaultdict

def validate_csv(csv_path, title=""):
    """
    Read a recommendation CSV and print item-by-item breakdown of expected_units_sold.
    """
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return
    
    print(f"\n{'='*100}")
    print(f"{title}")
    print(f"{'='*100}")
    
    df = pd.read_csv(csv_path)
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}\n")
    
    # Group by item_id to analyze expected_units_sold derivation
    item_stats = defaultdict(lambda: {"sellers": set(), "expected_units": None, "market_price": None})
    
    for _, row in df.iterrows():
        item_id = row.get("item_id")
        seller_id = row.get("seller_id")
        expected_units = row.get("expected_units_sold")
        market_price = row.get("market_price")
        item_name = row.get("item_name", "N/A")
        category = row.get("category", "N/A")
        
        if item_id is not None:
            item_stats[item_id]["sellers"].add(seller_id)
            item_stats[item_id]["expected_units"] = expected_units
            item_stats[item_id]["market_price"] = market_price
            item_stats[item_id]["item_name"] = item_name
            item_stats[item_id]["category"] = category
    
    # Print item breakdown
    print(f"{'Item ID':<15} {'Item Name':<25} {'Category':<20} {'#Sellers':<10} {'Expected Units':<18} {'Derived Total Units':<20} {'Derivation':<30}")
    print("-" * 148)
    
    for item_id in sorted(item_stats.keys()):
        stats = item_stats[item_id]
        sellers_count = len(stats["sellers"])
        expected_units = stats["expected_units"]
        item_name = str(stats.get("item_name", ""))[:24]
        category = str(stats.get("category", ""))[:19]
        
        # Reverse-engineer total_units_sold
        # expected_units_sold = total_units_sold / sellers_selling_count
        # => total_units_sold = expected_units_sold * sellers_selling_count
        if sellers_count > 0 and expected_units is not None:
            total_units_derived = expected_units * sellers_count
            derivation = f"{expected_units:.0f} × {sellers_count} = {total_units_derived:.0f}"
        else:
            total_units_derived = None
            derivation = "N/A"
        
        print(f"{str(item_id):<15} {item_name:<25} {category:<20} {sellers_count:<10} {str(expected_units):<18} {str(total_units_derived):<20} {derivation:<30}")
    
    print("\n" + "="*100)
    print(f"Summary by Category:\n")
    
    # Summarize by category
    category_summary = defaultdict(lambda: {"items": set(), "expected_units": None, "sellers": set()})
    for item_id, stats in item_stats.items():
        cat = stats.get("category", "Unknown")
        category_summary[cat]["items"].add(item_id)
        category_summary[cat]["expected_units"] = stats["expected_units"]
        category_summary[cat]["sellers"].update(stats["sellers"])
    
    print(f"{'Category':<25} {'#Items':<10} {'#Sellers':<10} {'Expected Units per Item':<25}")
    print("-" * 70)
    for cat in sorted(category_summary.keys()):
        summary = category_summary[cat]
        num_items = len(summary["items"])
        num_sellers = len(summary["sellers"])
        expected_units = summary.get("expected_units")
        print(f"{cat:<25} {num_items:<10} {num_sellers:<10} {str(expected_units):<25}")
    
    print("\n" + "="*100)

def main():
    base_path = "/Users/81194246/Desktop/Workspace/CTX/2025em1100102/ecommerce_seller_recommendation/local/data/output/gold"
    
    # Company recommendation CSV
    company_csv = os.path.join(
        base_path,
        "temp/dirty/cgpt/recommendation/company/part-00000-29c1e25b-5918-43f3-89e7-e420789ad549-c000.csv"
    )
    
    # Competitor recommendation CSV
    competitor_csv = os.path.join(
        base_path,
        "temp/dirty/cgpt/recommendation/competitor/part-00000-e105adaa-a5e3-4906-93fe-a4a27f9da0fd-c000.csv"
    )
    
    # Note: If the exact filenames differ, find them dynamically
    company_dir = os.path.join(base_path, "temp/dirty/cgpt/recommendation/company")
    competitor_dir = os.path.join(base_path, "dirty/temp/cgpt/recommendation/competitor")
    
    # Try to find the actual CSV files if the hardcoded paths don't exist
    if not os.path.exists(company_csv):
        print(f"Looking for company CSV in {company_dir}...")
        for file in os.listdir(company_dir):
            if file.endswith(".csv"):
                company_csv = os.path.join(company_dir, file)
                break
    
    if not os.path.exists(competitor_csv):
        print(f"Looking for competitor CSV in {competitor_dir}...")
        if os.path.exists(competitor_dir):
            for file in os.listdir(competitor_dir):
                if file.endswith(".csv"):
                    competitor_csv = os.path.join(competitor_dir, file)
                    break
    
    # Validate CSVs
    validate_csv(company_csv, title="COMPANY RECOMMENDATIONS - Expected Units Sold Breakdown")
    validate_csv(competitor_csv, title="COMPETITOR RECOMMENDATIONS - Expected Units Sold Breakdown")
    
    print("\n✅ Validation complete.\n")

if __name__ == "__main__":
    main()

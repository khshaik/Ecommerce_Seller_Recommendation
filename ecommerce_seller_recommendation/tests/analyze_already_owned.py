#!/usr/bin/env python3
"""
analyze_already_owned.py

Computes the seller-item pairs from seller catalog that match top-10 items per category,
showing which pairs were excluded from recommendations due to already being owned.
"""

import csv
from collections import defaultdict
from pathlib import Path

SELLER_CATALOG = '/Users/81194246/Desktop/Workspace/CTX/2025em1100102/ecommerce_seller_recommendation/local/data/input/dirty/seller/seller_catalog_dirty.csv'
COMPANY_SALES = '/Users/81194246/Desktop/Workspace/CTX/2025em1100102/ecommerce_seller_recommendation/local/data/input/dirty/company/company_sales_dirty.csv'
COMPETITOR_SALES = '/Users/81194246/Desktop/Workspace/CTX/2025em1100102/ecommerce_seller_recommendation/local/data/input/dirty/competitor/competitor_sales_dirty.csv'

def load_catalog_map():
    """Load item_id -> category mapping from seller catalog."""
    d = {}
    with open(SELLER_CATALOG, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            item = (row.get('item_id') or '').strip()
            cat = (row.get('category') or '').strip()
            if item and cat:
                d[item] = cat
    return d

def compute_top_n_items(sales_file, item_category_map, top_n=10):
    """
    Read sales file, aggregate units per item, and return top-N items per category.
    Returns: dict {item_id: category}
    """
    units = defaultdict(float)
    with open(sales_file, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            item = (row.get('item_id') or '').strip()
            if not item:
                continue
            try:
                u = float(row.get('units_sold') or 0)
            except:
                u = 0.0
            units[item] += u

    # Build per-category lists
    by_cat = defaultdict(list)
    for item, tot in units.items():
        cat = item_category_map.get(item)
        if cat:
            by_cat[cat].append((item, tot))

    # Top-N per category
    top_items = {}
    for cat, items in by_cat.items():
        items.sort(key=lambda x: x[1], reverse=True)
        for item, tot in items[:top_n]:
            top_items[item] = cat

    return top_items

def find_already_owned(top_items):
    """
    Find seller-item pairs in catalog that match top items.
    Returns: list of (seller_id, item_id, category, item_name, marketplace_price)
    """
    result = []
    with open(SELLER_CATALOG, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            item = (row.get('item_id') or '').strip()
            if item in top_items:
                seller = (row.get('seller_id') or '').strip()
                item_name = (row.get('item_name') or '').strip()
                price = (row.get('marketplace_price') or '').strip()
                cat = top_items[item]
                result.append({
                    'seller_id': seller,
                    'item_id': item,
                    'category': cat,
                    'item_name': item_name,
                    'marketplace_price': price
                })
    return result

def print_table(title, rows):
    """Print rows as a formatted table."""
    if not rows:
        print(f"\n{title}")
        print("  (empty)")
        return

    print(f"\n{title}")
    print(f"  Total rows: {len(rows)}\n")

    # Group by category for clarity
    by_cat = defaultdict(list)
    for row in rows:
        by_cat[row['category']].append(row)

    for cat in sorted(by_cat.keys()):
        items = by_cat[cat]
        print(f"  === {cat} ===")
        print(f"  {'Seller':<12} | {'Item ID':<12} | {'Item Name':<30} | {'Price':<10}")
        print("  " + "-" * 80)
        for row in sorted(items, key=lambda x: (x['seller_id'], x['item_id'])):
            print(f"  {row['seller_id']:<12} | {row['item_id']:<12} | {row['item_name']:<30} | {row['marketplace_price']:<10}")
        print()

def main():
    print("=" * 100)
    print("ALREADY-OWNED SELLER-ITEM PAIRS ANALYSIS")
    print("=" * 100)

    # Load seller catalog item->category map
    item_category_map = load_catalog_map()
    print(f"\nLoaded {len(item_category_map)} unique items from seller catalog")

    # ========== COMPANY ==========
    print("\n" + "=" * 100)
    print("COMPANY RECOMMENDATION")
    print("=" * 100)

    company_top = compute_top_n_items(COMPANY_SALES, item_category_map, top_n=10)
    print(f"  Top-10 items per category identified: {len(company_top)} items")
    
    # Show which categories & counts
    cat_counts = defaultdict(int)
    for item, cat in company_top.items():
        cat_counts[cat] += 1
    for cat in sorted(cat_counts.keys()):
        print(f"    {cat}: {cat_counts[cat]} items")

    company_already_owned = find_already_owned(company_top)
    print(f"  Already-owned pairs: {len(company_already_owned)}")

    print_table("COMPANY: Seller-Item Pairs Already Owned (excluded from 2040 candidate pairs)", company_already_owned)

    # ========== COMPETITOR ==========
    print("\n" + "=" * 100)
    print("COMPETITOR RECOMMENDATION")
    print("=" * 100)

    competitor_top = compute_top_n_items(COMPETITOR_SALES, item_category_map, top_n=10)
    print(f"  Top-10 items per category identified: {len(competitor_top)} items")
    
    # Show which categories & counts
    cat_counts = defaultdict(int)
    for item, cat in competitor_top.items():
        cat_counts[cat] += 1
    for cat in sorted(cat_counts.keys()):
        print(f"    {cat}: {cat_counts[cat]} items")

    competitor_already_owned = find_already_owned(competitor_top)
    print(f"  Already-owned pairs: {len(competitor_already_owned)}")

    print_table("COMPETITOR: Seller-Item Pairs Already Owned (excluded from 2040 candidate pairs)", competitor_already_owned)

    # ========== SUMMARY ==========
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"""
COMPANY RECOMMENDATION:
  - Total sellers: 51
  - Top-10 items per category: {len(company_top)} items
  - Candidate pairs: 51 × {len(company_top)} = {51 * len(company_top)}
  - Already-owned pairs (removed): {len(company_already_owned)}
  - Final recommendations: {51 * len(company_top) - len(company_already_owned)}

COMPETITOR RECOMMENDATION:
  - Total sellers: 51
  - Top-10 items per category: {len(competitor_top)} items
  - Candidate pairs: 51 × {len(competitor_top)} = {51 * len(competitor_top)}
  - Already-owned pairs (removed): {len(competitor_already_owned)}
  - Final recommendations: {51 * len(competitor_top) - len(competitor_already_owned)}
""")

if __name__ == '__main__':
    main()

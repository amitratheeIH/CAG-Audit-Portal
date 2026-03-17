#!/usr/bin/env python3
"""
invalidate_cache.py
-------------------
Invalidates Redis cache keys after report ingestion.

Keys invalidated:
  toc:<product_id>             — table-of-contents cache
  filters:<product_id>         — filter options cache
  report_meta:<product_id>     — metadata cache
  global:filter_options        — global aggregated filter options

Usage:
    python scripts/invalidate_cache.py --product-ids CAG-2023-CIVIL-UP-01
    python scripts/invalidate_cache.py --product-ids "ID1,ID2"
    python scripts/invalidate_cache.py --all-globals
"""

__version__ = "1.0"


import argparse
import os
import sys


GLOBAL_KEYS = [
    "global:filter_options",
    "global:report_list",
    "global:sector_counts",
]

PER_REPORT_KEY_PATTERNS = [
    "toc:{product_id}",
    "filters:{product_id}",
    "report_meta:{product_id}",
    "atn_summary:{product_id}",
]


def get_redis_client():
    try:
        import redis
    except ImportError:
        print("ERROR: redis package not installed. Run: pip install redis")
        sys.exit(1)
    url = os.environ.get("REDIS_URL")
    if not url:
        print("ERROR: REDIS_URL environment variable not set")
        sys.exit(1)
    return redis.from_url(url)


def main():
    parser = argparse.ArgumentParser(description="Invalidate Redis cache after ingestion")
    parser.add_argument("--product-ids", help="Comma-separated product_ids")
    parser.add_argument("--all-globals", action="store_true", help="Only invalidate global keys")
    args = parser.parse_args()

    r = get_redis_client()
    deleted = 0

    # Per-report keys
    if args.product_ids:
        for pid in args.product_ids.split(","):
            pid = pid.strip()
            keys = [p.format(product_id=pid) for p in PER_REPORT_KEY_PATTERNS]
            n = r.delete(*keys)
            deleted += n
            print(f"  {pid}: deleted {n} key(s)")

    # Global keys
    n = r.delete(*GLOBAL_KEYS)
    deleted += n
    print(f"  global: deleted {n} key(s)")

    print(f"\nTotal keys invalidated: {deleted}")


if __name__ == "__main__":
    main()

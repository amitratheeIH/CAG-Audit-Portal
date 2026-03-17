#!/usr/bin/env python3
"""
resolve_product_ids.py
----------------------
Reads a file containing changed file paths (one per line),
finds the product_id for each changed report file,
and outputs: ids=ID1,ID2 for GitHub Actions $GITHUB_OUTPUT
"""

__version__ = "1.0"

import sys
import pathlib

changed_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/changed_files.txt"

try:
    lines = pathlib.Path(changed_file).read_text().splitlines()
except FileNotFoundError:
    print("ids=")
    sys.exit(0)

repo_root = pathlib.Path(__file__).resolve().parent
ids = set()

for line in lines:
    line = line.strip()
    if not line:
        continue
    p = repo_root / line
    for parent in p.parents:
        if (parent / "manifest.json").exists():
            ids.add(parent.name)
            break

print(f"ids={','.join(sorted(ids))}")

#!/usr/bin/env python3
"""
test_atlas_search.py
--------------------
Tests all 3 Atlas Search indexes:
  1. block_search     — full-text search on block_vectors
  2. block_vector_index — vector/semantic search on block_vectors
  3. report_search    — full-text search on catalog_index

Usage:
    MONGODB_URI="..." COHERE_API_KEY="..." python test_atlas_search.py
"""

import json
import os
import sys

MONGODB_URI  = os.environ.get("MONGODB_URI")
COHERE_KEY   = os.environ.get("COHERE_API_KEY")
DB_NAME      = "cag_audit"

if not MONGODB_URI:
    print("ERROR: Set MONGODB_URI environment variable")
    sys.exit(1)

from pymongo import MongoClient
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

PASS = "✅"
FAIL = "❌"

def divider(title):
    print()
    print("─" * 60)
    print(f"  {title}")
    print("─" * 60)

# ── TEST 1: Full-text search on block_vectors ─────────────────────────────────
divider("TEST 1 — block_search (full-text on block_vectors)")

try:
    results = list(db.block_vectors.aggregate([
        {
            "$search": {
                "index": "block_search",
                "text": {
                    "query": "cess collection",
                    "path": "text_snippet"
                }
            }
        },
        { "$limit": 3 },
        { "$project": {
            "_id": 0,
            "block_id": 1,
            "unit_id": 1,
            "block_type": 1,
            "score": { "$meta": "searchScore" },
            "snippet": { "$substr": ["$text_snippet", 0, 120] }
        }}
    ]))

    if results:
        print(f"{PASS} Found {len(results)} results for 'cess collection'")
        for r in results:
            print(f"   [{r['score']:.3f}] {r['block_id']} — {r['snippet']}...")
    else:
        print(f"{FAIL} No results returned — check index is Active")

except Exception as e:
    print(f"{FAIL} Error: {e}")

# ── TEST 2: Filter + full-text ─────────────────────────────────────────────
divider("TEST 2 — block_search with filter (block_type=paragraph)")

try:
    results = list(db.block_vectors.aggregate([
        {
            "$search": {
                "index": "block_search",
                "compound": {
                    "must": [{
                        "text": {
                            "query": "registration workers",
                            "path": "text_snippet"
                        }
                    }],
                    "filter": [{
                        "equals": {
                            "path": "block_type",
                            "value": "paragraph"
                        }
                    }]
                }
            }
        },
        { "$limit": 3 },
        { "$project": {
            "_id": 0,
            "block_id": 1,
            "block_type": 1,
            "score": { "$meta": "searchScore" },
            "snippet": { "$substr": ["$text_snippet", 0, 120] }
        }}
    ]))

    if results:
        print(f"{PASS} Found {len(results)} paragraph blocks matching 'registration workers'")
        for r in results:
            print(f"   [{r['score']:.3f}] {r['block_id']} — {r['snippet']}...")
    else:
        print(f"{FAIL} No results")

except Exception as e:
    print(f"{FAIL} Error: {e}")

# ── TEST 3: report_search ─────────────────────────────────────────────────────
divider("TEST 3 — report_search (full-text on catalog_index)")

try:
    results = list(db.catalog_index.aggregate([
        {
            "$search": {
                "index": "report_search",
                "text": {
                    "query": "construction workers welfare",
                    "path": ["title.en", "summary.en"]
                }
            }
        },
        { "$limit": 3 },
        { "$project": {
            "_id": 0,
            "product_id": 1,
            "year": 1,
            "score": { "$meta": "searchScore" },
            "title": 1
        }}
    ]))

    if results:
        print(f"{PASS} Found {len(results)} reports matching 'construction workers welfare'")
        for r in results:
            title = r.get('title', {})
            t = title.get('en', str(title))[:80] if isinstance(title, dict) else str(title)[:80]
            print(f"   [{r['score']:.3f}] {r['product_id']} ({r.get('year')}) — {t}...")
    else:
        print(f"{FAIL} No results")

except Exception as e:
    print(f"{FAIL} Error: {e}")

# ── TEST 4: Vector search ─────────────────────────────────────────────────────
divider("TEST 4 — block_vector_index (semantic search)")

if not COHERE_KEY:
    print("⚠️  COHERE_API_KEY not set — skipping vector search test")
else:
    try:
        import cohere
        co = cohere.Client(api_key=COHERE_KEY)

        # Embed the query using search_query input_type
        query = "workers not receiving welfare benefits"
        resp = co.embed(
            texts=[query],
            model="embed-multilingual-v3.0",
            input_type="search_query"
        )
        query_vector = resp.embeddings[0]

        results = list(db.block_vectors.aggregate([
            {
                "$vectorSearch": {
                    "index": "block_vector_index",
                    "path": "embedding",
                    "queryVector": query_vector,
                    "numCandidates": 50,
                    "limit": 3
                }
            },
            { "$project": {
                "_id": 0,
                "block_id": 1,
                "unit_id": 1,
                "score": { "$meta": "vectorSearchScore" },
                "snippet": { "$substr": ["$text_snippet", 0, 120] }
            }}
        ]))

        if results:
            print(f"{PASS} Found {len(results)} semantically similar blocks")
            print(f"   Query: '{query}'")
            for r in results:
                print(f"   [{r['score']:.4f}] {r['block_id']} — {r['snippet']}...")
        else:
            print(f"{FAIL} No results — check vector index is Active and embedding dims=1024")

    except Exception as e:
        print(f"{FAIL} Error: {e}")

# ── TEST 5: Faceted search (finding categories) ───────────────────────────────
divider("TEST 5 — Faceted count by audit_findings_categories")

try:
    # Count blocks per finding category using aggregation
    results = list(db.block_vectors.aggregate([
        { "$match": { "resolved_meta.audit_findings_categories": { "$exists": True } } },
        { "$unwind": "$resolved_meta.audit_findings_categories" },
        { "$group": { "_id": "$resolved_meta.audit_findings_categories", "count": { "$sum": 1 } } },
        { "$sort": { "count": -1 } },
        { "$limit": 8 }
    ]))

    if results:
        print(f"{PASS} Finding categories across all blocks:")
        for r in results:
            print(f"   {r['_id']:45s} {r['count']} blocks")
    else:
        print(f"{FAIL} No results")

except Exception as e:
    print(f"{FAIL} Facet error (may need facet fields in index definition): {e}")

print()
print("─" * 60)
print("  Done")
print("─" * 60)
print()

#!/usr/bin/env python3
"""
generate_embeddings.py
----------------------
v1.0: Generates text-embedding-3-large embeddings for content blocks and writes
embedding sidecar files.

Sidecar format (one line per block):
  {"block_id": "...", "embedding": [...3072 floats...]}

Files:
  {report_dir}/embeddings/embeddings_{stem}.ndjson
  {report_dir}/embeddings/embeddings_{stem}.checksums.json

Where {stem} mirrors the source file's stem:
  blocks/content_block_ch02.ndjson → embeddings/embeddings_ch02.ndjson

embeddings/ is pipeline-generated and .gitignore'd.

v1.0 changes:
  - list block sub_items: items are now strict {text: {lang: str}} objects
    (the over-permissive open-object first variant was removed in content_block.schema v1.0).
    _get_sub_item_text() handles the new structure correctly.
  - executive_summary_block: body[] already handled correctly (v3.7 format)

Usage:
    python scripts/generate_embeddings.py --product-id AR06-CAG-2023-STATE-MP
    python scripts/generate_embeddings.py --product-ids "ID1,ID2"
    python scripts/generate_embeddings.py --all
    python scripts/generate_embeddings.py --all --force
"""

__version__ = "1.0"

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_layout as rl

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMS  = 3072
BATCH_SIZE      = 100
RATE_LIMIT_SLEEP = 0.5


def get_openai_client():
    try:
        from openai import OpenAI
    except ImportError:
        print("ERROR: openai package not installed. Run: pip install openai")
        sys.exit(1)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        sys.exit(1)
    return OpenAI(api_key=api_key)


def _multilingual_str(obj) -> str:
    """Extract English text from a multilingual {lang: str} object, or return str as-is."""
    if isinstance(obj, dict):
        return obj.get("en") or next(iter(obj.values()), "")
    return str(obj) if obj else ""


def _get_sub_item_text(sub: dict | str) -> str:
    """
    Extract text from a list sub_item.

    v1.0 schema: sub_items are strict {text: {lang: str}} objects.
    Legacy forms (bare strings or open-object dicts) are handled for backward
    compatibility with any pre-v1.0 block files still in the corpus.
    """
    if isinstance(sub, str):
        return sub
    if isinstance(sub, dict):
        # v1.0 canonical form: {text: {en: "...", hi: "..."}}
        text_field = sub.get("text")
        if text_field is not None:
            return _multilingual_str(text_field)
        # Legacy open-object form (pre-v1.0): {"en": "...", "hi": "..."}
        # Treat the whole object as a multilingual string
        return _multilingual_str(sub)
    return ""


def build_embedding_text(block: dict) -> str:
    """
    Concatenate meaningful text fields from a content block for embedding.

    Dispatch order:
      richbox / executive_summary_block — title + body[] (heading, paragraph, bullets, ordered_list, image items)
      paragraph               — content.text
      heading                 — content.text
      list                    — content.items[] with {text:{}, sub_items:[{text:{}}]}
      table                   — headers + first 10 rows + caption
      callout / sidebar       — content.title + content.text
      image / figure / map    — content.caption + content.alt_text
      audit_finding           — title, observation, effect, cause, recommendation
      recommendation          — content.text
      (fallback)              — content.text if present

    Note: richbox (v1.2+) body[] supports heading, paragraph, bullets, ordered_list, image, table_ref.
    executive_summary_block (deprecated alias) supports paragraph and bullets.

    Note: list block items use the v1.0 strict form {text: multilingual_obj,
    sub_items: [{text: multilingual_obj}], para_number: str, footnote_markers: []}.
    The open-object first variant was removed in content_block.schema v1.0.
    """
    parts = []
    block_type = block.get("block_type", "")
    content = block.get("content", {})

    if block_type in ("richbox", "executive_summary_block"):
        # richbox (v1.2+) and executive_summary_block (deprecated alias):
        # body[] is an ordered array of typed items.
        # richbox supports: heading, paragraph, bullets, ordered_list, image, table_ref
        # executive_summary_block supports: paragraph, bullets (legacy)
        if title := content.get("title"):
            parts.append(_multilingual_str(title))
        for item in content.get("body", []):
            item_type = item.get("type")
            if item_type in ("heading", "paragraph"):
                t = _multilingual_str(item.get("text", {}))
                if t:
                    parts.append(t)
            elif item_type in ("bullets", "ordered_list"):
                for bullet in item.get("items", []):
                    t = _multilingual_str(bullet.get("text", {}))
                    if t:
                        parts.append(t)
                    for sub in bullet.get("sub_items", []):
                        s = _get_sub_item_text(sub)
                        if s:
                            parts.append(s)
            elif item_type == "image":
                if cap := item.get("caption"):
                    parts.append(_multilingual_str(cap))
                if alt := item.get("alt_text"):
                    parts.append(_multilingual_str(alt))
            # table_ref: no text content to embed — skip
        if title := content.get("title"):
            parts.append(_multilingual_str(title))
        # v3.7 body[]: ordered array of typed items — paragraph | bullets
        for item in content.get("body", []):
            item_type = item.get("type")
            if item_type == "paragraph":
                t = _multilingual_str(item.get("text", {}))
                if t:
                    parts.append(t)
            elif item_type == "bullets":
                for bullet in item.get("items", []):
                    t = _multilingual_str(bullet.get("text", {}))
                    if t:
                        parts.append(t)
                    for sub in bullet.get("sub_items", []):
                        s = _get_sub_item_text(sub)
                        if s:
                            parts.append(s)

    elif block_type in ("paragraph", "heading", "pullquote", "quote"):
        if text := content.get("text"):
            parts.append(_multilingual_str(text))

    elif block_type in ("callout", "sidebar"):
        if title := content.get("title"):
            parts.append(_multilingual_str(title))
        if text := content.get("text"):
            parts.append(_multilingual_str(text))

    elif block_type == "list":
        # v1.0 list items: {text: {lang: str}, sub_items: [{text: {lang: str}}], ...}
        for item in content.get("items", [])[:20]:
            if isinstance(item, dict):
                t = _multilingual_str(item.get("text", {}))
                if t:
                    parts.append(t)
                # v1.0: sub_items are {text: multilingual_obj} objects
                for sub in item.get("sub_items", []):
                    s = _get_sub_item_text(sub)
                    if s:
                        parts.append(s)
            elif isinstance(item, str):
                # Legacy bare-string form — kept for backward compatibility
                parts.append(item)

    elif block_type == "table":
        if table := content.get("table"):
            headers = table.get("headers", [])
            if headers:
                parts.append(" | ".join(str(h) for h in headers))
            for row in table.get("rows", [])[:10]:
                parts.append(" | ".join(str(c) for c in row.get("cells", [])))
        if caption := content.get("caption"):
            parts.append(_multilingual_str(caption))

    elif block_type in ("image", "figure", "map", "chart"):
        if caption := content.get("caption"):
            parts.append(_multilingual_str(caption))
        if alt := content.get("alt_text"):
            parts.append(_multilingual_str(alt))

    elif block_type == "audit_finding":
        for field in ("title", "observation", "effect", "cause", "recommendation"):
            val = content.get(field)
            if val:
                parts.append(_multilingual_str(val))

    elif block_type == "recommendation":
        if text := content.get("text"):
            parts.append(_multilingual_str(text))

    else:
        if text := content.get("text"):
            parts.append(_multilingual_str(text))

    return "\n".join(p for p in parts if p).strip()


def embed_batch(client, texts: list[str]) -> list[list[float]]:
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
        dimensions=EMBEDDING_DIMS,
    )
    return [item.embedding for item in response.data]


def text_checksum(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def sidecar_stem(source_ndjson: Path) -> str:
    stem = source_ndjson.stem  # e.g. "content_block_ch02"
    return stem.replace("content_block_", "embeddings_", 1)


def process_report(report_dir: Path, client, force: bool) -> dict:
    stats = {"blocks": 0, "embedded": 0, "skipped": 0, "errors": 0}

    block_files = rl.block_ndjson_files(report_dir)
    if not block_files:
        print(f"    WARN: no content_block_*.ndjson files in {rl.blocks_dir(report_dir)}")
        return stats

    emb_dir = rl.embeddings_dir(report_dir)
    emb_dir.mkdir(exist_ok=True)

    for ndjson_path in block_files:
        stem = sidecar_stem(ndjson_path)
        sidecar_path   = emb_dir / f"{stem}.ndjson"
        checksum_path  = emb_dir / f"{stem}.checksums.json"

        existing: dict[str, str] = {}
        if sidecar_path.exists() and checksum_path.exists() and not force:
            try:
                existing = json.loads(checksum_path.read_text())
            except (json.JSONDecodeError, OSError):
                existing = {}

        blocks = []
        for line in ndjson_path.read_text().splitlines():
            line = line.strip()
            if line:
                try:
                    blocks.append(json.loads(line))
                except json.JSONDecodeError:
                    stats["errors"] += 1
        stats["blocks"] += len(blocks)

        to_embed: list[tuple[int, dict, str, str]] = []
        for idx, block in enumerate(blocks):
            block_id = block.get("block_id", f"block_{idx}")
            text = build_embedding_text(block)
            if not text:
                stats["skipped"] += 1
                continue
            chk = text_checksum(text)
            if not force and existing.get(block_id) == chk:
                stats["skipped"] += 1
                continue
            to_embed.append((idx, block, text, chk))

        if not to_embed:
            continue

        sidecar_data: dict[str, list[float]] = {}
        if sidecar_path.exists():
            for line in sidecar_path.read_text().splitlines():
                if line.strip():
                    try:
                        row = json.loads(line)
                        sidecar_data[row["block_id"]] = row["embedding"]
                    except (json.JSONDecodeError, KeyError):
                        pass

        new_checksums = dict(existing)

        for batch_start in range(0, len(to_embed), BATCH_SIZE):
            batch = to_embed[batch_start:batch_start + BATCH_SIZE]
            texts = [item[2] for item in batch]
            try:
                embeddings = embed_batch(client, texts)
            except Exception as exc:
                print(f"    ERROR batch starting at {batch_start}: {exc}")
                stats["errors"] += len(batch)
                time.sleep(2)
                continue

            for (idx, block, text, chk), embedding in zip(batch, embeddings):
                block_id = block.get("block_id", f"block_{idx}")
                sidecar_data[block_id] = embedding
                new_checksums[block_id] = chk
                stats["embedded"] += 1

            time.sleep(RATE_LIMIT_SLEEP)

        with sidecar_path.open("w") as f:
            for block in blocks:
                bid = block.get("block_id")
                if bid and bid in sidecar_data:
                    f.write(json.dumps({"block_id": bid, "embedding": sidecar_data[bid]}) + "\n")

        checksum_path.write_text(json.dumps(new_checksums, indent=2))
        print(f"    {ndjson_path.name}: {len(to_embed)} to embed, {stats['embedded']} done")

    return stats


def resolve_dirs(args) -> list[Path]:
    if args.all:
        return rl.all_report_dirs()
    ids = []
    if args.product_ids:
        ids = [pid.strip() for pid in args.product_ids.split(",")]
    elif args.product_id:
        ids = [args.product_id.strip()]
    dirs = []
    for pid in ids:
        found = rl.locate_report(pid)
        if not found:
            print(f"WARN: product_id '{pid}' not found — skipping")
        else:
            dirs.append(found)
    return dirs


def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for content blocks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-id", help="Single product_id")
    group.add_argument("--product-ids", help="Comma-separated product_ids")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--force", action="store_true", help="Re-embed even if checksum matches")
    args = parser.parse_args()

    client = get_openai_client()
    dirs = resolve_dirs(args)

    if not dirs:
        print("No report directories to process.")
        sys.exit(0)

    total = {"blocks": 0, "embedded": 0, "skipped": 0, "errors": 0}

    for report_dir in dirs:
        try:
            label = str(report_dir.relative_to(rl.REPO_ROOT))
        except ValueError:
            label = str(report_dir)
        print(f"\nProcessing {label} ...")
        stats = process_report(report_dir, client, args.force)
        for k in total:
            total[k] += stats[k]

    print(f"\n{'─'*60}")
    print(f"Total blocks : {total['blocks']}")
    print(f"Embedded     : {total['embedded']}")
    print(f"Skipped      : {total['skipped']}  (checksum match or no text)")
    print(f"Errors       : {total['errors']}")

    if total["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Generate OpenAI embeddings for the Enron email corpus."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from openai import AsyncOpenAI

from enron_emails.embed_chunker import (
    chunk_text,
    estimate_tokens,
    needs_chunking,
    prepare_text,
)

MODEL = "text-embedding-3-small"
DIMENSIONS = 1536
MAX_INPUT_TOKENS = 8191
MAX_INPUT_CHARS = MAX_INPUT_TOKENS * 4  # conservative char limit
MAX_TOKENS_PER_REQUEST = 50_000
MAX_CONCURRENT = 10
MAX_RETRIES = 5


def _load_client() -> AsyncOpenAI:
    """Create an async OpenAI client, loading .env if present."""
    load_dotenv()
    import os

    if not os.environ.get("OPENAI_API_KEY"):
        msg = "OPENAI_API_KEY not set. Add it to .env or export it."
        raise RuntimeError(msg)
    return AsyncOpenAI(max_retries=MAX_RETRIES)


def build_adaptive_batches(
    texts: list[str],
    max_tokens: int = MAX_TOKENS_PER_REQUEST,
    max_items: int = 2048,
) -> list[list[int]]:
    """Group text indices into batches respecting token and item limits."""
    batches: list[list[int]] = []
    current_batch: list[int] = []
    current_tokens = 0

    for i, text in enumerate(texts):
        tokens = estimate_tokens(text)
        if current_batch and (
            current_tokens + tokens > max_tokens or len(current_batch) >= max_items
        ):
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0
        current_batch.append(i)
        current_tokens += tokens

    if current_batch:
        batches.append(current_batch)
    return batches


def _is_embeddable(text: str) -> bool:
    """Check if text has enough real content to embed."""
    stripped = text.strip()
    if not stripped:
        return False
    # Must have at least some alphanumeric content
    alnum = sum(1 for c in stripped if c.isalnum())
    return alnum >= 3


async def _embed_batch(
    client: AsyncOpenAI,
    texts: list[str],
    semaphore: asyncio.Semaphore,
) -> list[list[float] | None]:
    """Embed a single batch of texts, respecting concurrency limits."""
    try:
        async with semaphore:
            response = await client.embeddings.create(
                model=MODEL,
                input=texts,
                dimensions=DIMENSIONS,
            )
        sorted_data = sorted(response.data, key=lambda d: d.index)
        return [d.embedding for d in sorted_data]
    except (ValueError, Exception) as exc:
        # If batch fails, fall back to one-at-a-time
        print(f"    Batch of {len(texts)} failed ({exc}), retrying individually...")
        results: list[list[float] | None] = []
        for text in texts:
            try:
                async with semaphore:
                    resp = await client.embeddings.create(
                        model=MODEL,
                        input=[text],
                        dimensions=DIMENSIONS,
                    )
                results.append(resp.data[0].embedding)
            except Exception:
                results.append(None)
        return results


async def embed_texts(
    client: AsyncOpenAI,
    texts: list[str],
    *,
    concurrency: int = MAX_CONCURRENT,
    progress_label: str = "",
) -> list[list[float] | None]:
    """Embed a list of texts, skipping empty strings (which get None).

    Returns embeddings in the same order as input texts.
    """
    # Identify texts with real content, truncate to token limit
    non_empty: list[tuple[int, str]] = []
    for i, t in enumerate(texts):
        if _is_embeddable(t):
            # Truncate to stay within the model's token limit
            if len(t) > MAX_INPUT_CHARS:
                t = t[:MAX_INPUT_CHARS]
            non_empty.append((i, t))

    # Pre-fill results with None
    results: list[list[float] | None] = [None] * len(texts)

    if not non_empty:
        return results

    non_empty_indices = [idx for idx, _ in non_empty]
    non_empty_texts = [t for _, t in non_empty]

    # Build adaptive batches over the non-empty texts
    batches = build_adaptive_batches(non_empty_texts)
    semaphore = asyncio.Semaphore(concurrency)

    total = len(non_empty_texts)
    done = 0
    prefix = f"[{progress_label}] " if progress_label else ""

    for batch_num, batch_indices in enumerate(batches):
        batch_texts = [non_empty_texts[i] for i in batch_indices]
        embeddings = await _embed_batch(client, batch_texts, semaphore)

        for local_idx, emb in zip(batch_indices, embeddings):
            original_idx = non_empty_indices[local_idx]
            results[original_idx] = emb

        done += len(batch_indices)
        if (batch_num + 1) % 10 == 0 or done == total:
            print(f"  {prefix}{done:,}/{total:,} texts embedded", flush=True)

    return results


def _read_messages(input_path: Path, custodian: str | None = None) -> pl.DataFrame:
    """Read eml_messages parquet, optionally filtered by custodian."""
    df = pl.read_parquet(input_path)
    if custodian:
        df = df.filter(pl.col("custodian") == custodian)
    return df


def _checkpoint_path(output_dir: Path, custodian: str, column: str) -> Path:
    """Path for a custodian's embedding checkpoint file."""
    return output_dir / column / f"{custodian}.parquet"


def _is_complete(checkpoint: Path, expected_rows: int) -> bool:
    """Check if a checkpoint file exists with the expected row count."""
    if not checkpoint.exists():
        return False
    try:
        schema = pl.read_parquet_schema(checkpoint)
        # Quick row count check
        df = pl.scan_parquet(checkpoint).select(pl.len()).collect()
        return df.item() == expected_rows and "embedding" in schema
    except Exception:
        return False


async def _embed_custodian_async(
    client: AsyncOpenAI,
    df: pl.DataFrame,
    custodian: str,
    column: str,
    output_dir: Path,
) -> Path:
    """Embed a single custodian's texts and write checkpoint."""
    checkpoint = _checkpoint_path(output_dir, custodian, column)

    if _is_complete(checkpoint, df.height):
        print(f"  {custodian}: already complete ({df.height:,} rows), skipping")
        return checkpoint

    # Prepare texts
    subjects = df["subject_clean"].to_list()
    bodies = df[column].to_list()
    texts = [
        prepare_text(subj, body)
        for subj, body in zip(subjects, bodies)
    ]

    embeddings = await embed_texts(client, texts, progress_label=custodian)

    result = pl.DataFrame({
        "doc_id": df["doc_id"],
        "custodian": df["custodian"],
        "embedding": pl.Series(
            "embedding",
            [e if e is not None else None for e in embeddings],
            dtype=pl.List(pl.Float32),
        ),
    })

    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(checkpoint)
    return checkpoint


def embed_custodian(
    custodian: str,
    input_path: Path,
    output_dir: Path,
    *,
    column: str = "body_top",
) -> Path:
    """Embed a single custodian's emails. Sync wrapper."""
    df = _read_messages(input_path, custodian)
    if df.height == 0:
        print(f"  {custodian}: no messages found, skipping")
        return _checkpoint_path(output_dir, custodian, column)

    client = _load_client()
    return asyncio.run(
        _embed_custodian_async(client, df, custodian, column, output_dir)
    )


async def _embed_all_async(
    input_path: Path,
    output_dir: Path,
    *,
    column: str = "body_top",
    custodians: list[str] | None = None,
) -> Path:
    """Embed all custodians, with per-custodian checkpointing."""
    df = pl.read_parquet(input_path)
    all_custodians = sorted(df["custodian"].unique().to_list())
    if custodians:
        all_custodians = [c for c in all_custodians if c in custodians]

    client = _load_client()
    total_embedded = 0

    for i, custodian in enumerate(all_custodians):
        cust_df = df.filter(pl.col("custodian") == custodian)
        print(
            f"[{i + 1}/{len(all_custodians)}] {custodian} "
            f"({cust_df.height:,} emails)...",
            flush=True,
        )
        await _embed_custodian_async(client, cust_df, custodian, column, output_dir)
        total_embedded += cust_df.height

    # Consolidate checkpoints
    consolidated = _consolidate(output_dir, column)
    print(f"Consolidated {total_embedded:,} embeddings -> {consolidated}")
    return consolidated


def _consolidate(output_dir: Path, column: str) -> Path:
    """Merge per-custodian checkpoints into one parquet."""
    checkpoint_dir = output_dir / column
    parts = sorted(checkpoint_dir.glob("*.parquet"))
    if not parts:
        msg = f"No checkpoint files found in {checkpoint_dir}"
        raise FileNotFoundError(msg)

    frames = [pl.read_parquet(p) for p in parts]
    combined = pl.concat(frames)

    # Rename embedding column to include the source column name
    combined = combined.rename({"embedding": f"embedding_{column}"})

    out_path = output_dir / f"eml_embeddings_{column}.parquet"
    combined.write_parquet(out_path)
    return out_path


def embed_all(
    input_path: Path,
    output_dir: Path,
    *,
    column: str = "body_top",
    custodians: list[str] | None = None,
) -> Path:
    """Embed all custodians. Sync entry point."""
    return asyncio.run(
        _embed_all_async(input_path, output_dir, column=column, custodians=custodians)
    )


async def _build_chunks_async(
    input_path: Path,
    output_path: Path,
) -> Path:
    """Build chunked embeddings for long emails."""
    df = pl.read_parquet(input_path)

    # Filter to emails that need chunking
    rows: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        text = prepare_text(row.get("subject_clean"), row.get("body"))
        if not text.strip() or not needs_chunking(text):
            continue
        chunks = chunk_text(text)
        for idx, chunk in enumerate(chunks):
            rows.append({
                "doc_id": row["doc_id"],
                "custodian": row["custodian"],
                "chunk_index": idx,
                "chunk_text": chunk,
            })

    if not rows:
        print("No emails need chunking.")
        empty = pl.DataFrame(
            schema={
                "doc_id": pl.Utf8,
                "custodian": pl.Utf8,
                "chunk_index": pl.UInt16,
                "chunk_text": pl.Utf8,
                "embedding": pl.List(pl.Float32),
            }
        )
        empty.write_parquet(output_path)
        return output_path

    chunk_df = pl.DataFrame(rows).cast({"chunk_index": pl.UInt16})
    print(f"  {chunk_df.height:,} chunks from {chunk_df['doc_id'].n_unique():,} emails")

    # Embed all chunks
    client = _load_client()
    texts = chunk_df["chunk_text"].to_list()
    embeddings = await embed_texts(client, texts, progress_label="chunks")

    chunk_df = chunk_df.with_columns(
        pl.Series(
            "embedding",
            [e if e is not None else None for e in embeddings],
            dtype=pl.List(pl.Float32),
        )
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_df.write_parquet(output_path)
    return output_path


def build_chunks(input_path: Path, output_path: Path) -> Path:
    """Build chunked embeddings. Sync entry point."""
    return asyncio.run(_build_chunks_async(input_path, output_path))

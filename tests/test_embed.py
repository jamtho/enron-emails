"""Tests for embedding module."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from enron_emails.embed import build_adaptive_batches, embed_texts


class TestBuildAdaptiveBatches:
    def test_small_texts_single_batch(self) -> None:
        texts = ["hello"] * 10
        batches = build_adaptive_batches(texts)
        assert len(batches) == 1
        assert batches[0] == list(range(10))

    def test_respects_token_limit(self) -> None:
        # Each text ~250 tokens, limit is 600 -> 2 per batch
        texts = ["x" * 1000] * 6
        batches = build_adaptive_batches(texts, max_tokens=600)
        assert len(batches) == 3
        for batch in batches:
            assert len(batch) == 2

    def test_respects_item_limit(self) -> None:
        texts = ["hi"] * 100
        batches = build_adaptive_batches(texts, max_items=30)
        assert all(len(b) <= 30 for b in batches)

    def test_empty_input(self) -> None:
        assert build_adaptive_batches([]) == []

    def test_single_large_text(self) -> None:
        texts = ["x" * 200000]  # ~50k tokens
        batches = build_adaptive_batches(texts, max_tokens=50000)
        assert len(batches) == 1
        assert batches[0] == [0]


class TestEmbedTexts:
    @pytest.mark.asyncio
    async def test_skips_empty_texts(self) -> None:
        mock_client = AsyncMock()

        def make_response(*, model: str, input: list[str], dimensions: int) -> MagicMock:
            resp = MagicMock()
            resp.data = [
                MagicMock(index=i, embedding=[1.0] * 10)
                for i in range(len(input))
            ]
            return resp

        mock_client.embeddings.create.side_effect = make_response

        results = await embed_texts(
            mock_client, ["hello", "", "  ", "world"], concurrency=1
        )

        assert results[0] is not None
        assert results[1] is None  # empty
        assert results[2] is None  # whitespace only
        assert results[3] is not None
        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_all_empty(self) -> None:
        mock_client = AsyncMock()
        results = await embed_texts(mock_client, ["", "", ""], concurrency=1)
        assert all(r is None for r in results)
        mock_client.embeddings.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_preserves_order(self) -> None:
        mock_client = AsyncMock()

        def make_response(texts: list[str], **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.data = [
                MagicMock(index=i, embedding=[float(hash(t) % 100)] * 10)
                for i, t in enumerate(texts)
            ]
            return resp

        mock_client.embeddings.create.side_effect = (
            lambda *, model, input, dimensions: make_response(input)
        )

        texts = [f"text_{i}" for i in range(5)]
        results = await embed_texts(mock_client, texts, concurrency=1)

        assert len(results) == 5
        assert all(r is not None for r in results)
        # Each embedding should be distinct
        embeddings = [r[0] for r in results if r is not None]
        assert len(set(embeddings)) > 1

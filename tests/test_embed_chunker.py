"""Tests for text preparation and chunking."""

import pytest

from enron_emails.embed_chunker import (
    chunk_text,
    estimate_tokens,
    needs_chunking,
    prepare_text,
)


class TestPrepareText:
    def test_subject_and_body(self) -> None:
        result = prepare_text("Meeting tomorrow", "Let's meet at 3pm.")
        assert result == "Subject: Meeting tomorrow\n\nLet's meet at 3pm."

    def test_body_only(self) -> None:
        result = prepare_text(None, "Just a body.")
        assert result == "Just a body."

    def test_subject_only(self) -> None:
        result = prepare_text("Hello", None)
        assert result == "Subject: Hello"

    def test_both_empty(self) -> None:
        assert prepare_text(None, None) == ""
        assert prepare_text("", "") == ""

    def test_strips_whitespace(self) -> None:
        result = prepare_text("  Hello  ", "  Body  ")
        assert result == "Subject: Hello\n\nBody"


class TestEstimateTokens:
    def test_short_text(self) -> None:
        assert estimate_tokens("hello") == 2

    def test_longer_text(self) -> None:
        text = "a" * 400
        assert estimate_tokens(text) == 101

    def test_empty(self) -> None:
        assert estimate_tokens("") == 1


class TestNeedsChunking:
    def test_short_text(self) -> None:
        assert needs_chunking("hello") is False

    def test_long_text(self) -> None:
        text = "word " * 10000  # ~50k chars, ~12.5k tokens
        assert needs_chunking(text) is True

    def test_at_boundary(self) -> None:
        # 8000 tokens * 4 chars = 32000 chars
        text = "a" * 31999
        assert needs_chunking(text) is False
        text = "a" * 32001
        assert needs_chunking(text) is True


class TestChunkText:
    def test_short_text_no_split(self) -> None:
        assert chunk_text("hello world") == ["hello world"]

    def test_empty_text(self) -> None:
        assert chunk_text("") == []
        assert chunk_text(None) == []  # type: ignore[arg-type]

    def test_splits_on_paragraph_boundary(self) -> None:
        para1 = "A" * 3000
        para2 = "B" * 3000
        para3 = "C" * 3000
        text = f"{para1}\n\n{para2}\n\n{para3}"
        chunks = chunk_text(text, max_chars=6500, overlap=500)
        assert len(chunks) >= 2
        # First chunk should contain para1
        assert "A" * 100 in chunks[0]
        # Last chunk should contain para3
        assert "C" * 100 in chunks[-1]

    def test_overlap_present(self) -> None:
        para1 = "First paragraph. " * 200  # ~3400 chars
        para2 = "Second paragraph. " * 200
        text = f"{para1}\n\n{para2}"
        chunks = chunk_text(text, max_chars=4000, overlap=500)
        assert len(chunks) >= 2
        # The end of chunk 0 should overlap with start of chunk 1
        tail = chunks[0][-200:]
        assert tail in chunks[1]

    def test_single_huge_paragraph(self) -> None:
        text = "word " * 5000  # ~25k chars, no paragraph breaks
        chunks = chunk_text(text, max_chars=6000, overlap=500)
        assert len(chunks) >= 4
        for chunk in chunks:
            assert len(chunk) <= 6500  # allow some slack for overlap joins

    def test_all_text_preserved(self) -> None:
        """Every word in the input should appear in at least one chunk."""
        words = [f"word{i}" for i in range(2000)]
        text = " ".join(words)
        chunks = chunk_text(text, max_chars=3000, overlap=500)
        all_chunk_text = " ".join(chunks)
        for w in words[:10] + words[-10:]:
            assert w in all_chunk_text


class TestChunkTextEdgeCases:
    @pytest.mark.parametrize("max_chars", [100, 500, 1000, 6000])
    def test_various_max_chars(self, max_chars: int) -> None:
        text = "Hello world. " * 500
        chunks = chunk_text(text, max_chars=max_chars, overlap=50)
        assert len(chunks) >= 1
        # All chunks within limit (with reasonable slack)
        for chunk in chunks:
            assert len(chunk) <= max_chars + 200

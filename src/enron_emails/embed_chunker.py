"""Text preparation and chunking for embedding generation."""

from __future__ import annotations


def prepare_text(subject: str | None, body: str | None) -> str:
    """Combine subject and body into a single string for embedding.

    Returns empty string if both are empty/null.
    """
    parts: list[str] = []
    if subject and subject.strip():
        parts.append(f"Subject: {subject.strip()}")
    if body and body.strip():
        parts.append(body.strip())
    return "\n\n".join(parts)


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4 + 1


def needs_chunking(text: str, max_tokens: int = 8000) -> bool:
    """Check if text exceeds the token limit and needs splitting."""
    return estimate_tokens(text) > max_tokens


def chunk_text(
    text: str,
    max_chars: int = 6000,
    overlap: int = 500,
) -> list[str]:
    """Split text into overlapping chunks for embedding.

    Tries to split on paragraph boundaries, falls back to sentence
    boundaries, then hard character splits.
    """
    if not text or len(text) <= max_chars:
        return [text] if text else []

    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        # Single paragraph exceeds max — split it further
        if para_len > max_chars:
            # Flush current buffer first
            if current:
                chunks.append("\n\n".join(current))
                current = []
                current_len = 0
            chunks.extend(_split_long_paragraph(para, max_chars, overlap))
            continue

        if current_len + para_len + 2 > max_chars and current:
            chunk_text_str = "\n\n".join(current)
            chunks.append(chunk_text_str)
            # Start next chunk with overlap from end of previous
            overlap_text = chunk_text_str[-overlap:] if len(chunk_text_str) > overlap else ""
            current = [overlap_text, para] if overlap_text else [para]
            current_len = len(overlap_text) + para_len + 2
        else:
            current.append(para)
            current_len += para_len + 2

    if current:
        chunks.append("\n\n".join(current))

    return [c for c in chunks if c.strip()]


def _split_long_paragraph(text: str, max_chars: int, overlap: int) -> list[str]:
    """Split a single long paragraph on sentence boundaries, then hard split."""
    sentences = _split_sentences(text)
    if len(sentences) <= 1:
        return _hard_split(text, max_chars, overlap)

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for sent in sentences:
        if len(sent) > max_chars:
            if current:
                chunks.append(" ".join(current))
                current = []
                current_len = 0
            chunks.extend(_hard_split(sent, max_chars, overlap))
            continue

        if current_len + len(sent) + 1 > max_chars and current:
            chunk_str = " ".join(current)
            chunks.append(chunk_str)
            overlap_text = chunk_str[-overlap:] if len(chunk_str) > overlap else ""
            current = [overlap_text + " " + sent] if overlap_text else [sent]
            current_len = len(current[0])
        else:
            current.append(sent)
            current_len += len(sent) + 1

    if current:
        chunks.append(" ".join(current))

    return [c for c in chunks if c.strip()]


def _split_sentences(text: str) -> list[str]:
    """Naive sentence splitter on '. ', '? ', '! '."""
    import re

    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p for p in parts if p.strip()]


def _hard_split(text: str, max_chars: int, overlap: int) -> list[str]:
    """Last resort: split on character boundaries with overlap."""
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + max_chars
        chunks.append(text[start:end])
        start = end - overlap
    return chunks

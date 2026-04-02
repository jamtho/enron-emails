"""Parse .eml files from the EDRM Enron corpus into structured Polars DataFrames.

Overview
========

The EDRM Enron v2 dataset stores emails as individual RFC 2822 ``.eml`` files
under ``data/unpacked/{custodian}/native_*/*.eml``.  This module reads those
files, extracts structured fields, and writes the result to Parquet.

Two output tables are produced:

* **eml_messages.parquet** — one row per email
* **eml_attachments.parquet** — one row per MIME attachment part

What is extracted
-----------------

From each ``.eml`` file we extract:

**Envelope / headers:**

* ``message_id`` — the Message-ID header (present on every email)
* ``date`` — parsed to ``datetime[us, UTC]`` via ``email.utils.parsedate_to_datetime``
* ``from_raw`` — the original From header verbatim
* ``from_name`` — display name, normalised (see address parsing below)
* ``from_email`` — email address where available (null for bare-name senders)
* ``to_raw``, ``cc_raw`` — original To / CC headers verbatim
* ``to_addrs``, ``cc_addrs`` — semicolon-delimited parsed recipient lists
* ``subject`` — the raw Subject header
* ``subject_clean`` — subject with leading ``RE:``, ``FW:``, ``Fwd:`` chains stripped
* ``is_reply``, ``is_forward`` — flags derived from the subject prefix

**Body:**

* ``body`` — the full ``text/plain`` body with the EDRM dataset footer and the
  Enron Corp confidentiality disclaimer stripped.  Every email in the corpus has
  a ``text/plain`` part, so HTML-to-text conversion is never needed.
* ``body_top`` — only the "new" text above the first reply/forward separator.
  This is the portion the sender actually wrote, excluding quoted material.
* ``reply_depth`` — count of embedded reply/forward separators in the body
  (``-----Original Message-----``, ``--- Forwarded by``, and blocks of ``>``
  quoting each count as one level).

**Metadata (from X-headers):**

* ``folder`` — from ``X-Folder``, the Outlook/Notes folder the email lived in
* ``source_file`` — from ``X-Filename``, the ``.pst`` or ``.nsf`` archive name
* ``x_sdoc``, ``x_zlid`` — EDRM document identifiers

**Attachments (from MIME structure):**

* ``filename``, ``mime_type``, ``size`` — per attachment part
* ``parent_doc_id`` — links back to the message

**Derived:**

* ``doc_id`` — extracted from the ``.eml`` filename
* ``custodian`` — the custodian directory name
* ``has_attachments``, ``attachment_count`` — from MIME part inspection
* ``native_path`` — relative path to the ``.eml`` file

Address parsing
---------------

The corpus contains four distinct From/To formats:

1. **Standard RFC 2822**: ``"Display Name" <user@domain.com>`` or
   ``Display Name <user@domain.com>`` — we extract both name and email.

2. **Bare display names**: ``Jeff Dasovich`` — common for internal Enron
   senders.  We store the name; ``from_email`` is null.

3. **X.500 distinguished names**: ``/O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC``
   — we extract the final CN value as the name.

4. **IMCEANOTES-encoded**: ``IMCEANOTES-user+40domain+2Ecom+40ENRON@ENRON.com``
   — an Exchange artefact.  We decode the ``+XX`` hex escapes to recover the
   original address.

5. **Empty** — ~21% of emails (calendar entries, stubs) have no From header.
   These are included with null address fields.

What is NOT extracted (left in the files)
-----------------------------------------

* **HTML bodies** — a few emails have ``text/html`` parts alongside the
  ``text/plain``.  We always prefer ``text/plain`` and ignore the HTML.
  No emails in the corpus are HTML-only.

* **Attachment content** — binary attachment payloads (Word docs, Excel
  spreadsheets, PDFs, images) are not extracted.  Only metadata (filename,
  MIME type, size) is stored.  The original files remain in ``native_*/``.

* **Embedded message parts** — ``message/rfc822`` parts (forwarded-as-
  attachment emails) are catalogued as attachments but not recursively parsed.

* **Full MIME headers on parts** — per-part Content-ID, Content-Disposition
  parameters, etc. are not preserved.

* **Received / routing headers** — the ``Received:`` chain, ``Return-Path``,
  authentication headers are not extracted (only ~3% of emails have them).

* **Rare X-headers** — ``X-Mailer``, ``X-Priority``, ``X-MSMail-Priority``,
  ``Importance``, ``Sender``, ``Reply-To`` appear on <1% of emails and are
  not extracted.

* **Threading headers** — ``In-Reply-To``, ``References``, ``Thread-Topic``,
  ``Thread-Index`` are absent from the entire corpus (zero occurrences in
  sampling), so there is nothing to extract.

* **The EDRM footer text** — stripped from ``body`` and ``body_top``.  The
  original is always available in the ``.eml`` / ``.txt`` files.

* **Quoted/forwarded content** — present in ``body`` but stripped from
  ``body_top``.  The original quoted text remains in ``body`` and in the
  source files.

* **Attachment file paths** — the XML manifests (parsed by ``xml_metadata.py``)
  record the path to attachment files on disk.  This module does not duplicate
  that; it only records what MIME tells us.
"""

from __future__ import annotations

import email
import email.policy
import email.utils
import re
from datetime import UTC, datetime
from pathlib import Path  # noqa: TC003 — used at runtime
from typing import Any

import polars as pl

# ---------------------------------------------------------------------------
# Address parsing
# ---------------------------------------------------------------------------

# Matches standard "Name <email>" or <email> forms — anchored to the LAST <...>
_ANGLE_RE = re.compile(r"^(.*?)\s*<([^>]+)>\s*$")

# X.500 DN — extract final CN= segment
_X500_RE = re.compile(r"/CN=([^/]+)$", re.IGNORECASE)

# Exchange-style name fragments: <Last>,"First" or <Last>,<First>
_EXCHANGE_NAME_RE = re.compile(r"<([^>]+)>\s*,\s*[\"<]?([^>\"]+)[>\"]?")

# Exchange gateway routing: Name/Org@Org pattern.
# Detects strings with /OrgPath and @OrgSuffix where the @ part has no dots
# (i.e. not a real email domain).
_GATEWAY_RE = re.compile(
    r"^(?P<name>.+?)"               # display name (non-greedy)
    r"(?:/[^@\s]+)*"                 # /Org path components
    r"(?:@[A-Za-z][A-Za-z0-9_]*)+$" # @Org routing hops (no dots — not a domain)
)

# Strip remaining angle brackets and quotes from display names
_NAME_JUNK_RE = re.compile(r"[<>\"]")

# IMCEANOTES encoding: +XX hex pairs
_IMCEA_RE = re.compile(r"IMCEANOTES-(.+?)@", re.IGNORECASE)
_IMCEA_HEX_RE = re.compile(r"\+([0-9A-Fa-f]{2})")


def _decode_imceanotes(s: str) -> str:
    """Decode IMCEANOTES +XX hex escapes back to characters."""
    m = _IMCEA_RE.search(s)
    if not m:
        return s
    encoded = m.group(1)
    return _IMCEA_HEX_RE.sub(lambda h: chr(int(h.group(1), 16)), encoded)


def _clean_display_name(name: str) -> str | None:
    """Clean up a display name extracted from an address field.

    Handles Exchange-style fragments like ``<Harris>,"Steven"`` → ``Steven Harris``
    and strips stray angle brackets, quotes, and excess whitespace.
    """
    if not name:
        return None

    # Exchange-style: <Last>,"First" or <Last>,<First>
    m = _EXCHANGE_NAME_RE.match(name.strip())
    if m:
        last, first = m.group(1).strip(), m.group(2).strip()
        return f"{first} {last}"

    # Strip leftover angle brackets and quotes
    cleaned = _NAME_JUNK_RE.sub("", name).strip().strip(",").strip()
    # Normalise "Last,First" → "Last, First"
    cleaned = re.sub(r",\s*", ", ", cleaned)

    # Collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned)

    return cleaned or None


def parse_address(raw: str) -> tuple[str | None, str | None]:
    """Parse an email address string into (display_name, email_address).

    Handles the four formats found in the Enron corpus:
    - Standard RFC 2822: "Name" <email> → (Name, email)
    - Bare display name: Jeff Dasovich → (Jeff Dasovich, None)
    - X.500 DN: /O=ENRON/.../CN=JDASOVIC → (JDASOVIC, None)
    - IMCEANOTES: encoded Exchange address → decoded (name, email)
    - Empty string → (None, None)
    """
    if not raw or not raw.strip():
        return None, None

    raw = raw.strip()

    # IMCEANOTES-encoded address (possibly wrapped in "Name" <IMCEANOTES-...>)
    if "IMCEANOTES" in raw:
        # If it's in angle-bracket form, preserve the display name
        m = _ANGLE_RE.match(raw)
        outer_name = None
        if m:
            outer_name = _clean_display_name(m.group(1))
            raw_inner = m.group(2)
        else:
            raw_inner = raw
        decoded = _decode_imceanotes(raw_inner)
        inner_name, inner_email = parse_address(decoded)
        # Prefer the outer display name if available
        return outer_name or inner_name, inner_email

    # Angle-bracket form: "Name" <email> or Name <email> or <email>
    m = _ANGLE_RE.match(raw)
    if m:
        name_raw = m.group(1).strip()
        addr = m.group(2).strip()

        # If the "address" has no @ and isn't an X.500 DN, the angle brackets
        # are just Exchange name formatting (e.g. <Mangin>,<Emmanuel>).
        # Treat the whole thing as a display name instead.
        if "@" not in addr and "/O=" not in addr.upper() and "/CN=" not in addr.upper():
            return _clean_display_name(_NAME_JUNK_RE.sub("", raw)), None

        # The "name" part might itself be an X.500 DN
        if name_raw and "/O=" in name_raw.upper():
            cn = _X500_RE.search(name_raw)
            name = cn.group(1) if cn else _clean_display_name(name_raw)
        else:
            name = _clean_display_name(name_raw)

        # The "addr" part might be an X.500 DN rather than an email
        if "/O=" in addr.upper() or "/CN=" in addr.upper():
            cn = _X500_RE.search(addr)
            return name or (cn.group(1) if cn else None), None

        return name, addr

    # X.500 distinguished name (no angle brackets)
    if "/O=" in raw.upper() or "/CN=" in raw.upper():
        cn = _X500_RE.search(raw)
        return (cn.group(1) if cn else raw, None)

    # Exchange gateway routing: Name/Org@Org (no dots in @-parts)
    m = _GATEWAY_RE.match(raw)
    if m:
        stripped = m.group("name").rstrip(".- ")
        if stripped:
            return parse_address(stripped)
        return None, None

    # Bare email address
    if "@" in raw and " " not in raw:
        return None, raw

    # Bare display name (the most common case for internal senders)
    return raw, None


def parse_address_list(raw: str) -> list[str]:
    """Parse a comma/semicolon-separated address list into individual strings.

    Returns the list of raw individual address strings, preserving the original
    format.  Splitting is aware of angle brackets (won't split on commas inside
    ``<...>``).
    """
    if not raw or not raw.strip():
        return []

    results: list[str] = []
    depth = 0
    current: list[str] = []

    for ch in raw:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth = max(0, depth - 1)
            current.append(ch)
        elif ch in (",", ";") and depth == 0:
            token = "".join(current).strip()
            if token:
                results.append(token)
            current = []
        else:
            current.append(ch)

    token = "".join(current).strip()
    if token:
        results.append(token)

    return results


# ---------------------------------------------------------------------------
# Subject cleaning
# ---------------------------------------------------------------------------

_SUBJECT_PREFIX_RE = re.compile(
    r"^(\s*(?:RE|FW|Fwd)\s*:\s*)+", re.IGNORECASE
)


def clean_subject(subject: str) -> tuple[str, bool, bool]:
    """Strip RE:/FW:/Fwd: prefixes from a subject line.

    Returns (cleaned_subject, is_reply, is_forward).
    """
    if not subject:
        return "", False, False

    prefix_match = _SUBJECT_PREFIX_RE.match(subject)
    if not prefix_match:
        return subject.strip(), False, False

    prefix = prefix_match.group(0).upper()
    is_reply = "RE" in prefix
    is_forward = "FW" in prefix
    cleaned = subject[prefix_match.end():].strip()
    return cleaned, is_reply, is_forward


# ---------------------------------------------------------------------------
# Body extraction and cleaning
# ---------------------------------------------------------------------------

# The EDRM footer appears on every email.  It starts with a line of asterisks
# followed by "EDRM Enron Email Data Set" on the next line.
_EDRM_FOOTER_RE = re.compile(
    r"\n\*{5,}\s*\n\s*EDRM Enron Email Data Set.*",
    re.DOTALL,
)

# The Enron Corp confidentiality disclaimer (multiple wrapping variants).
_ENRON_DISCLAIMER_RE = re.compile(
    r"\n\*{10,}\s*\n\s*This e-mail is the property of Enron Corp\..*?\*{10,}",
    re.DOTALL,
)

# Reply / forward separators — used for splitting body_top from quoted text.
_REPLY_SEPARATOR_RE = re.compile(
    r"(?:"
    r"\s*-----\s*Original Message\s*-----"
    r"|"
    r"\s*-{10,}\s*Forwarded\s"
    r")",
    re.IGNORECASE,
)

# A block of > quoting: 3+ consecutive lines starting with >
_QUOTE_BLOCK_RE = re.compile(
    r"(?:^>.*\n){3,}",
    re.MULTILINE,
)


def extract_body(msg: email.message.Message) -> str:
    """Get the text/plain body from an email Message, decoded to str."""
    if msg.is_multipart():
        for part in msg.walk():
            if (
                part.get_content_type() == "text/plain"
                and part.get_content_disposition() != "attachment"
            ):
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def strip_footers(body: str) -> str:
    """Remove the EDRM dataset footer and Enron confidentiality disclaimer."""
    body = _ENRON_DISCLAIMER_RE.sub("", body)
    body = _EDRM_FOOTER_RE.sub("", body)
    return body.rstrip()


def split_reply(body: str) -> tuple[str, int]:
    """Split the body into the "new" top portion and count reply depth.

    Returns (body_top, reply_depth).

    ``body_top`` is the text the sender actually wrote — everything above
    the first reply/forward separator.  ``reply_depth`` counts how many
    separator markers appear in the full body:

    * Each ``-----Original Message-----`` counts as 1
    * Each ``--- Forwarded by`` block counts as 1
    * A contiguous block of ``>``-quoted lines counts as 1

    These can overlap in deeply nested threads, so the count is additive.
    """
    depth = 0

    # Count Original Message markers
    depth += len(_REPLY_SEPARATOR_RE.findall(body))

    # Count > quote blocks (3+ consecutive lines)
    depth += len(_QUOTE_BLOCK_RE.findall(body))

    # Find the first separator to split body_top
    earliest = len(body)

    sep_match = _REPLY_SEPARATOR_RE.search(body)
    if sep_match:
        earliest = min(earliest, sep_match.start())

    # Also check for start of > quoting
    quote_match = _QUOTE_BLOCK_RE.search(body)
    if quote_match:
        earliest = min(earliest, quote_match.start())

    body_top = body[:earliest].rstrip()
    return body_top, depth


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def parse_date(raw: str | None) -> datetime | None:
    """Parse an RFC 2822 date string to a UTC datetime.

    Returns None for unparseable dates or obvious bad data (year < 1970).
    """
    if not raw:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        # Normalise to UTC
        dt = dt.astimezone(UTC)
        # Filter out obviously bad dates (year 0002, etc.)
        if dt.year < 1970 or dt.year > 2030:
            return None
        return dt
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Attachment extraction
# ---------------------------------------------------------------------------


def extract_attachments(
    msg: email.message.Message,
    doc_id: str,
    custodian: str,
) -> list[dict[str, Any]]:
    """Extract attachment metadata from MIME parts.

    Returns a list of dicts with keys: parent_doc_id, custodian, filename,
    mime_type, size.
    """
    attachments: list[dict[str, Any]] = []
    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        if part.is_multipart():
            continue

        ct = part.get_content_type()
        disp = part.get_content_disposition()

        # Skip the main text body
        if ct == "text/plain" and disp != "attachment":
            continue
        # Skip text/html inline bodies
        if ct == "text/html" and disp != "attachment":
            continue

        # Everything else is an attachment
        filename = part.get_filename() or ""
        payload = part.get_payload(decode=True)
        size = len(payload) if payload else 0

        attachments.append({
            "parent_doc_id": doc_id,
            "custodian": custodian,
            "filename": filename,
            "mime_type": ct,
            "size": size,
        })

    return attachments


# ---------------------------------------------------------------------------
# Single-email parser
# ---------------------------------------------------------------------------


def _doc_id_from_path(eml_path: Path) -> str:
    """Extract doc_id from the .eml filename.

    Filenames look like ``3.287079.LTUWB1UEUURY0AMLCGNSEUNK52PXR2CPB.eml``.
    The doc_id is the stem without the .eml extension.
    """
    return eml_path.stem


def parse_eml(eml_path: Path, custodian: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse a single .eml file into a message record and attachment records.

    Returns (message_dict, attachment_list).
    """
    doc_id = _doc_id_from_path(eml_path)
    raw_bytes = eml_path.read_bytes()
    msg = email.message_from_bytes(raw_bytes, policy=email.policy.compat32)

    # --- Headers ---
    message_id = msg.get("Message-ID", "")
    date_raw = msg.get("Date", "")
    from_raw = msg.get("From", "") or ""
    to_raw = msg.get("To", "") or ""
    cc_raw = msg.get("Cc", "") or msg.get("CC", "") or ""
    subject = msg.get("Subject", "") or ""

    # Address parsing
    from_name, from_email = parse_address(from_raw.strip())
    to_addrs = parse_address_list(to_raw)
    cc_addrs = parse_address_list(cc_raw)

    # Subject
    subject_clean, is_reply, is_forward = clean_subject(subject)

    # Date
    date = parse_date(date_raw)

    # --- Body ---
    body_raw = extract_body(msg)
    body = strip_footers(body_raw)
    body_top, reply_depth = split_reply(body)

    # --- Metadata ---
    folder = (msg.get("X-Folder", "") or "").strip()
    source_file = (msg.get("X-Filename", "") or "").strip()
    x_sdoc = (msg.get("X-SDOC", "") or "").strip()
    x_zlid = (msg.get("X-ZLID", "") or "").strip()

    # --- Attachments ---
    attachments = extract_attachments(msg, doc_id, custodian)

    # --- Relative path ---
    # Store path relative to the unpacked dir: e.g. native_000/filename.eml
    try:
        native_path = str(eml_path.relative_to(eml_path.parent.parent))
    except ValueError:
        native_path = eml_path.name

    message = {
        "doc_id": doc_id,
        "custodian": custodian,
        "message_id": message_id.strip(),
        "date": date,
        "from_raw": from_raw.strip(),
        "from_name": from_name,
        "from_email": from_email,
        "to_raw": to_raw.strip(),
        "to_addrs": ";".join(to_addrs),
        "cc_raw": cc_raw.strip(),
        "cc_addrs": ";".join(cc_addrs),
        "subject": subject.strip(),
        "subject_clean": subject_clean,
        "is_reply": is_reply,
        "is_forward": is_forward,
        "body": body,
        "body_top": body_top,
        "reply_depth": reply_depth,
        "folder": folder,
        "source_file": source_file,
        "x_sdoc": x_sdoc,
        "x_zlid": x_zlid,
        "has_attachments": len(attachments) > 0,
        "attachment_count": len(attachments),
        "native_path": native_path,
    }

    return message, attachments


# ---------------------------------------------------------------------------
# Batch parsing
# ---------------------------------------------------------------------------

_MESSAGES_SCHEMA = {
    "doc_id": pl.Utf8,
    "custodian": pl.Utf8,
    "message_id": pl.Utf8,
    "date": pl.Datetime("us", "UTC"),
    "from_raw": pl.Utf8,
    "from_name": pl.Utf8,
    "from_email": pl.Utf8,
    "to_raw": pl.Utf8,
    "to_addrs": pl.Utf8,
    "cc_raw": pl.Utf8,
    "cc_addrs": pl.Utf8,
    "subject": pl.Utf8,
    "subject_clean": pl.Utf8,
    "is_reply": pl.Boolean,
    "is_forward": pl.Boolean,
    "body": pl.Utf8,
    "body_top": pl.Utf8,
    "reply_depth": pl.UInt8,
    "folder": pl.Utf8,
    "source_file": pl.Utf8,
    "x_sdoc": pl.Utf8,
    "x_zlid": pl.Utf8,
    "has_attachments": pl.Boolean,
    "attachment_count": pl.UInt16,
    "native_path": pl.Utf8,
}

_ATTACHMENTS_SCHEMA = {
    "parent_doc_id": pl.Utf8,
    "custodian": pl.Utf8,
    "filename": pl.Utf8,
    "mime_type": pl.Utf8,
    "size": pl.UInt64,
}


def parse_custodian_emls(custodian_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse all .eml files in a custodian directory.

    Scans ``custodian_dir/native_*/*.eml`` and returns
    ``(messages_df, attachments_df)``.
    """
    custodian = custodian_dir.name
    messages: list[dict[str, Any]] = []
    attachments: list[dict[str, Any]] = []
    errors: list[str] = []

    eml_files = sorted(custodian_dir.glob("native_*/*.eml"))

    for eml_path in eml_files:
        try:
            msg, atts = parse_eml(eml_path, custodian)
            messages.append(msg)
            attachments.extend(atts)
        except Exception as e:
            errors.append(f"{eml_path.name}: {e}")

    if errors:
        import sys
        print(f"  {len(errors)} parse errors in {custodian}:", file=sys.stderr)
        for err in errors[:5]:
            print(f"    {err}", file=sys.stderr)
        if len(errors) > 5:
            print(f"    ... and {len(errors) - 5} more", file=sys.stderr)

    messages_df = (
        pl.DataFrame(messages, schema=_MESSAGES_SCHEMA)
        if messages
        else pl.DataFrame(schema=_MESSAGES_SCHEMA)
    )
    attachments_df = (
        pl.DataFrame(attachments, schema=_ATTACHMENTS_SCHEMA)
        if attachments
        else pl.DataFrame(schema=_ATTACHMENTS_SCHEMA)
    )

    return messages_df, attachments_df


def _parse_custodian_to_parquet(args: tuple[Path, Path]) -> str | None:
    """Worker: parse one custodian and write per-custodian parquet shards.

    Returns the custodian name on success, or None if no .eml files found.
    """
    custodian_dir, shard_dir = args
    if not list(custodian_dir.glob("native_*/*.eml")):
        return None

    name = custodian_dir.name
    messages_df, attachments_df = parse_custodian_emls(custodian_dir)
    print(f"  {name}: {messages_df.height:,} messages, {attachments_df.height:,} attachments")

    messages_df.write_parquet(shard_dir / f"{name}_messages.parquet")
    attachments_df.write_parquet(shard_dir / f"{name}_attachments.parquet")
    return name


def parse_all_emls(unpacked_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse all custodian directories under unpacked_dir.

    Uses multiprocessing (one worker per custodian, capped at CPU count)
    to parallelise parsing.  Each worker writes per-custodian parquet shards
    to a temporary directory; these are combined at the end and then deleted.

    Returns ``(messages_df, attachments_df)`` with all custodians combined.
    """
    import multiprocessing
    import shutil

    shard_dir = unpacked_dir.parent / "parquet" / "_eml_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)

    custodian_dirs = sorted(
        d for d in unpacked_dir.iterdir() if d.is_dir()
    )

    workers = min(multiprocessing.cpu_count(), len(custodian_dirs))
    print(f"Parsing {len(custodian_dirs)} custodians with {workers} workers...")

    with multiprocessing.Pool(workers) as pool:
        results = pool.map(
            _parse_custodian_to_parquet,
            [(d, shard_dir) for d in custodian_dirs],
        )

    parsed = [r for r in results if r is not None]
    print(f"Parsed {len(parsed)} custodians, combining shards...")

    msg_shards = sorted(shard_dir.glob("*_messages.parquet"))
    att_shards = sorted(shard_dir.glob("*_attachments.parquet"))

    messages = (
        pl.concat([pl.read_parquet(p) for p in msg_shards])
        if msg_shards
        else pl.DataFrame(schema=_MESSAGES_SCHEMA)
    )
    attachments = (
        pl.concat([pl.read_parquet(p) for p in att_shards])
        if att_shards
        else pl.DataFrame(schema=_ATTACHMENTS_SCHEMA)
    )

    # Clean up temporary shards
    shutil.rmtree(shard_dir)

    return messages, attachments


def write_eml_parquet(
    messages: pl.DataFrame,
    attachments: pl.DataFrame,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write messages and attachments DataFrames to Parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    msg_path = output_dir / "eml_messages.parquet"
    att_path = output_dir / "eml_attachments.parquet"
    messages.write_parquet(msg_path)
    attachments.write_parquet(att_path)
    return msg_path, att_path

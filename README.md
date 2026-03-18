# enron-emails

Preprocessing pipeline for the [EDRM Enron v2 dataset](https://archive.org/download/edrm.enron.email.data.set.v2.xml) — converts the raw XML/EML archive into clean Parquet data frames for analysis.

## Overview

The EDRM Enron v2 dataset contains ~159 zip archives (one per email custodian), each with:

- **XML manifests** — structured metadata per email (from, to, subject, date, attachments, folder, custodian, file hashes)
- **Native .eml files** — raw RFC 2822 email messages with full MIME structure
- **Pre-extracted .txt files** — plain text versions of each message

This project provides tooling to download, unpack, and parse these archives into queryable Parquet files using Polars and DuckDB.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-extras
```

## Usage

### CLI

```bash
# Download and unpack specific custodians
uv run enron-emails download harris-s allen-p skilling-j

# Parse to Parquet
uv run enron-emails parse-xml
uv run enron-emails parse-eml
uv run enron-emails parse-xml harris-s allen-p

# Download + parse in one step
uv run enron-emails pipeline harris-s allen-p
```

### Exploration tools

Install the optional terminal tools for hands-on browsing:

```bash
sudo apt install fd-find fzf bat ripgrep
uv tool install visidata
```

Then use the scripts in `bin/`:

| Script | What it does |
|---|---|
| `bin/email-browse [custodian]` | fzf picker over .txt files with bat preview |
| `bin/email-eml [custodian]` | Same but for raw .eml files |
| `bin/email-sample [N] [custodian]` | Dump N random emails to pager |
| `bin/email-search <pattern> [custodian]` | ripgrep search with fzf picker |
| `bin/email-patterns [custodian]` | Tally reply markers, boilerplate, encoding artefacts |

And `vd data/parquet/eml_messages.parquet` for interactive spreadsheet-style exploration of the parsed data.

## Data model

Two independent parsing pipelines produce four Parquet files in `data/parquet/`:

### XML pipeline (`parse-xml`)

Parses the XML manifests. Fast, but only extracts structured metadata — no email bodies.

**`xml_messages.parquet`** — one row per email

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | EDRM document ID |
| `custodian` | str | Custodian directory name |
| `custodian_raw` | str | Full custodian string from XML |
| `from_` | str | Sender (raw XML tag value) |
| `to` | str | Recipients |
| `cc` | str | CC recipients |
| `subject` | str | Subject line |
| `date_sent` | datetime | Parsed timestamp |
| `has_attachments` | bool | |
| `attachment_count` | u16 | |
| `attachment_names` | str | Semicolon-delimited |
| `x_sdoc` | str | EDRM source document ID |
| `x_zlid` | str | ZL Technologies ID |
| `location_uri` | str | Mailbox folder path |
| `native_path` | str | Path to .eml file |
| `native_hash` | str | MD5 of the .eml file |
| `native_file_size` | u32 | Bytes |
| `text_path` | str | Path to .txt extract |

**`xml_attachments.parquet`** — one row per attachment

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | Attachment document ID |
| `parent_doc_id` | str | Parent message doc_id |
| `custodian` | str | |
| `file_name` | str | Original filename |
| `file_extension` | str | |
| `file_size` | u64 | Bytes |
| `date_created` | datetime | |
| `date_modified` | datetime | |
| `mime_type` | str | |
| `native_path` | str | Path to attachment file |
| `native_hash` | str | |
| `text_path` | str | Path to text extract |

### EML pipeline (`parse-eml`)

Parses the raw .eml files directly with Python's `email` module. Extracts everything the XML pipeline does, plus full email bodies with reply chain analysis.

**`eml_messages.parquet`** — one row per email

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | From .eml filename |
| `custodian` | str | Custodian directory name |
| `message_id` | str | Message-ID header |
| `date` | datetime[us, UTC] | Parsed and normalised to UTC |
| `from_raw` | str | Original From header verbatim |
| `from_name` | str | Extracted display name (normalised) |
| `from_email` | str | Extracted email address (null for bare names) |
| `to_raw` | str | Original To header |
| `to_addrs` | str | Semicolon-delimited parsed recipient list |
| `cc_raw` | str | Original CC header |
| `cc_addrs` | str | Semicolon-delimited parsed CC list |
| `subject` | str | Raw subject |
| `subject_clean` | str | Subject with RE:/FW: prefixes stripped |
| `is_reply` | bool | Subject starts with RE: |
| `is_forward` | bool | Subject starts with FW:/Fwd: |
| `body` | str | Full text/plain body, EDRM footer stripped |
| `body_top` | str | Only the "new" text above the first reply separator |
| `reply_depth` | u8 | Count of embedded reply/forward markers |
| `folder` | str | X-Folder header (Outlook/Notes folder) |
| `source_file` | str | X-Filename header (the .pst/.nsf archive) |
| `x_sdoc` | str | EDRM source document ID |
| `x_zlid` | str | ZL Technologies ID |
| `has_attachments` | bool | From MIME structure |
| `attachment_count` | u16 | |
| `native_path` | str | Relative path to .eml file |

**`eml_attachments.parquet`** — one row per MIME attachment

| Column | Type | Description |
|---|---|---|
| `parent_doc_id` | str | Links to eml_messages.doc_id |
| `custodian` | str | |
| `filename` | str | MIME filename |
| `mime_type` | str | |
| `size` | u64 | Bytes |

### Address normalisation

The corpus contains several From/To address formats, all handled by the EML parser:

| Format | Example | Result |
|---|---|---|
| Standard RFC 2822 | `"Jeff Dasovich" <jdasovic@enron.com>` | name + email |
| Bare display name | `Jeff Dasovich` | name only |
| X.500 DN | `/O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC` | CN extracted as name |
| Exchange fragments | `<Harris>,"Steven" </O=ENRON/...>` | Reassembled as "Steven Harris" |
| IMCEANOTES-encoded | `IMCEANOTES-user+40domain+2Ecom@ENRON.com` | Hex decoded to `user@domain.com` |
| Empty (calendar stubs) | | null |

### Body processing

- **Footer stripping**: The universal EDRM dataset license footer and the Enron Corp confidentiality disclaimer are removed from `body` and `body_top`.
- **Reply splitting**: `body_top` contains only the sender's new text, split at the first `-----Original Message-----`, `--- Forwarded by`, or `>` quote block.
- **Reply depth**: Counts all reply/forward separators in the full body.
- **No HTML conversion needed**: Every email in the corpus has a `text/plain` part.

### What the EML parser does NOT extract

These remain in the raw .eml files under `data/unpacked/`:

- **HTML bodies** — ignored in favour of the always-present `text/plain`
- **Attachment content** — binary payloads (Word, Excel, PDF, images) are not extracted, only metadata
- **Embedded messages** — `message/rfc822` parts are catalogued but not recursively parsed
- **Routing headers** — `Received`, `Return-Path`, authentication headers (~3% of emails)
- **Rare headers** — `X-Mailer`, `X-Priority`, `Reply-To`, `Sender` (<1% of emails)
- **Threading headers** — `In-Reply-To`, `References` are absent from the entire corpus
- **The EDRM footer itself** — stripped from output, always recoverable from source files

### Querying with DuckDB

```sql
-- Top senders
SELECT from_name, from_email, count(*) as n
FROM 'data/parquet/eml_messages.parquet'
WHERE from_name IS NOT NULL
GROUP BY from_name, from_email
ORDER BY n DESC
LIMIT 20;

-- Emails per custodian
SELECT custodian, count(*) as msgs
FROM 'data/parquet/eml_messages.parquet'
GROUP BY custodian
ORDER BY msgs DESC;

-- Reply depth distribution
SELECT reply_depth, count(*) as n
FROM 'data/parquet/eml_messages.parquet'
GROUP BY reply_depth
ORDER BY reply_depth;
```

## Data directory layout

```
data/                   # gitignored
  downloads/            # Raw zip files from archive.org
  unpacked/             # Extracted per-custodian directories
    harris-s/
      native_000/       # .eml files and attachment files
      text_000/         # Pre-extracted .txt files
      *.xml             # XML manifests
  parquet/              # Output Parquet files
```

## Development

```bash
uv run pytest           # Run tests
uv run ruff check src/ tests/  # Lint
```

## Project structure

```
src/enron_emails/
  __init__.py
  cli.py              # CLI entry points (download, parse-xml, parse-eml, pipeline)
  download.py          # Download and unpack from archive.org
  xml_metadata.py      # XML manifest parser -> Polars -> Parquet
  eml_parse.py         # .eml file parser -> Polars -> Parquet
tests/
  test_smoke.py        # Environment smoke tests
  test_xml_metadata.py # XML parser tests
  test_eml_parse.py    # EML parser tests (39 tests)
bin/
  email-browse         # Interactive fzf browser for .txt files
  email-eml            # Interactive fzf browser for .eml files
  email-sample         # Random email sampler
  email-search         # ripgrep + fzf search
  email-patterns       # Boilerplate/reply pattern tally
```

# enron-emails

Preprocessing pipeline for the [EDRM Enron v2 dataset](https://archive.org/download/edrm.enron.email.data.set.v2.xml) — converts the raw XML/EML archive into clean Parquet data frames for analysis.

## Overview

The EDRM Enron v2 dataset contains ~159 zip archives (one per email custodian), each with:

- **XML manifests** — structured metadata per email (from, to, subject, date, attachments, folder, custodian, file hashes)
- **Native .eml files** — raw RFC 2822 email messages with full MIME structure
- **Pre-extracted .txt files** — plain text versions of each message

This project provides tooling to download, unpack, parse, and embed these archives into queryable Parquet files with vector embeddings for semantic search.

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

# Generate embeddings (requires OPENAI_API_KEY in .env)
uv run enron-emails embed                      # all custodians, body_top
uv run enron-emails embed skilling-j lay-k     # specific custodians
uv run enron-emails embed --body               # also embed full body
uv run enron-emails embed --chunks             # chunked embeddings for long emails

# Upload Parquet files to S3-compatible storage (requires S3 config in .env)
uv run enron-emails upload
```

### S3 configuration

The `upload` command pushes all generated Parquet files to an S3-compatible store (AWS S3, MinIO, etc.). Configure via `.env`:

```
S3_ENDPOINT_URL=http://localhost:9000
S3_BUCKET=enron-emails
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
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

Three pipelines produce Parquet files in `data/parquet/`:

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

### Embeddings (`embed`)

Generates OpenAI `text-embedding-3-small` (1536 dims) embeddings for email text. Prepends `subject_clean` to `body_top` for context. Stored in a separate parquet file that joins to `eml_messages` on `doc_id`.

Requires `OPENAI_API_KEY` in `.env`. Costs ~$5-6 for the full corpus.

**`eml_embeddings_body_top.parquet`** — one row per email

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | Links to eml_messages.doc_id |
| `custodian` | str | |
| `embedding_body_top` | list[f32] | 1536-dim vector (null if text was empty/not embeddable) |

Per-custodian checkpoints are stored in `eml_embeddings/body_top/` for resumability.

Optional `--chunks` flag produces **`eml_chunks.parquet`** with overlapping chunks for long emails:

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | Parent email doc_id |
| `custodian` | str | |
| `chunk_index` | u16 | 0-based chunk position |
| `chunk_text` | str | The chunk text |
| `embedding` | list[f32] | 1536-dim vector |

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

### Semantic search with embeddings

**Find similar emails with DuckDB:**

```sql
-- Pick a query email and find the 10 most similar
WITH query AS (
    SELECT embedding_body_top AS vec
    FROM 'data/parquet/eml_embeddings/eml_embeddings_body_top.parquet'
    WHERE doc_id = '3.287476.PJLLF2EF1DFZFLMEP0YOFS5WS3U50VABB'
)
SELECT m.custodian, m.from_name, m.subject_clean,
       list_dot_product(e.embedding_body_top, q.vec) AS similarity
FROM 'data/parquet/eml_embeddings/eml_embeddings_body_top.parquet' e,
     query q
JOIN 'data/parquet/eml_messages.parquet' m ON e.doc_id = m.doc_id
WHERE e.embedding_body_top IS NOT NULL
ORDER BY similarity DESC
LIMIT 10;
```

**Similarity search with Polars + NumPy:**

```python
import polars as pl
import numpy as np

emb = pl.read_parquet("data/parquet/eml_embeddings/eml_embeddings_body_top.parquet")
msgs = pl.read_parquet("data/parquet/eml_messages.parquet")

# Join embeddings with message metadata
df = emb.join(msgs.select("doc_id", "custodian", "from_name", "subject_clean", "body_top"), on="doc_id")

# Filter to rows with embeddings and build matrix
df = df.filter(pl.col("embedding_body_top").is_not_null())
vecs = np.array(df["embedding_body_top"].to_list(), dtype=np.float32)

# Search by email index
query_idx = 0
sims = vecs @ vecs[query_idx]
top_10 = np.argsort(sims)[::-1][:10]
for i in top_10:
    print(f"{sims[i]:.3f}  {df['from_name'][int(i)]:30s}  {df['subject_clean'][int(i)]}")
```

**Search by text query (embed a new string):**

```python
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI()

query_text = "California energy crisis power prices"
resp = client.embeddings.create(model="text-embedding-3-small", input=[query_text], dimensions=1536)
query_vec = np.array(resp.data[0].embedding, dtype=np.float32)

sims = vecs @ query_vec
top_10 = np.argsort(sims)[::-1][:10]
for i in top_10:
    print(f"{sims[i]:.3f}  {df['custodian'][int(i)]:15s}  {df['subject_clean'][int(i)]}")
```

**Cluster emails by topic:**

```python
from sklearn.cluster import KMeans

# Sample for speed (full matrix is large)
sample_idx = np.random.choice(len(vecs), size=50000, replace=False)
sample_vecs = vecs[sample_idx]

kmeans = KMeans(n_clusters=50, random_state=42, n_init=3)
labels = kmeans.fit_predict(sample_vecs)

# Show top subjects per cluster
sample_df = df[sample_idx.tolist()]
for cluster_id in range(5):
    mask = labels == cluster_id
    cluster_subjects = sample_df.filter(pl.Series(mask))["subject_clean"].drop_nulls().to_list()[:5]
    print(f"\nCluster {cluster_id}: {sum(mask)} emails")
    for s in cluster_subjects:
        print(f"  {s}")
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
    xml_messages.parquet
    xml_attachments.parquet
    eml_messages.parquet
    eml_attachments.parquet
    eml_embeddings/     # Embedding output
      body_top/         # Per-custodian checkpoints
      eml_embeddings_body_top.parquet  # Consolidated (6 GB)
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
  cli.py              # CLI entry points (download, parse-xml, parse-eml, embed, upload, pipeline)
  download.py          # Download and unpack from archive.org
  upload.py            # Upload Parquet files to S3-compatible storage
  xml_metadata.py      # XML manifest parser -> Polars -> Parquet
  eml_parse.py         # .eml file parser -> Polars -> Parquet
  embed.py             # OpenAI embedding generation with async batching
  embed_chunker.py     # Text preparation and chunking for long emails
tests/
  test_smoke.py        # Environment smoke tests
  test_xml_metadata.py # XML parser tests
  test_eml_parse.py    # EML parser tests
  test_embed.py        # Embedding module tests (mocked API)
  test_embed_chunker.py # Chunker unit tests
bin/
  email-browse         # Interactive fzf browser for .txt files
  email-eml            # Interactive fzf browser for .eml files
  email-sample         # Random email sampler
  email-search         # ripgrep + fzf search
  email-patterns       # Boilerplate/reply pattern tally
```

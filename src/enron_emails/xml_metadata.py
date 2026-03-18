"""Streaming XML parser for EDRM Enron email metadata."""

from pathlib import Path
from typing import Any
from xml.etree.ElementTree import Element, iterparse

import polars as pl

# Tag names that appear on Message documents
_MESSAGE_TAGS = frozenset({
    "#From", "#To", "#CC", "#Subject", "#DateSent",
    "#HasAttachments", "#AttachmentCount", "#AttachmentNames",
    "X-SDOC", "X-ZLID",
})

# Tag names that appear on File (attachment) documents
_FILE_TAGS = frozenset({
    "#FileName", "#FileExtension", "#FileSize",
    "#DateCreated", "#DateModified",
})


def _extract_files(doc: Element) -> dict[str, str | None]:
    """Extract native/text file paths, sizes, and hashes from a Document element."""
    result: dict[str, str | None] = {
        "native_path": None,
        "native_hash": None,
        "native_size": None,
        "text_path": None,
    }
    for file_el in doc.iter("File"):
        ftype = file_el.get("FileType")
        ext = file_el.find("ExternalFile")
        if ext is None:
            continue
        fpath = ext.get("FilePath", "")
        fname = ext.get("FileName", "")
        full_path = f"{fpath}/{fname}" if fpath else fname
        if ftype == "Native":
            result["native_path"] = full_path
            result["native_hash"] = ext.get("Hash")
            result["native_size"] = ext.get("FileSize")
        elif ftype == "Text":
            result["text_path"] = full_path
    return result


def _extract_location(doc: Element) -> dict[str, str | None]:
    """Extract custodian and folder location from a Document element."""
    loc = doc.find(".//Location")
    if loc is None:
        return {"custodian_raw": None, "location_uri": None}
    custodian_el = loc.find("Custodian")
    uri_el = loc.find("LocationURI")
    return {
        "custodian_raw": custodian_el.text if custodian_el is not None else None,
        "location_uri": uri_el.text if uri_el is not None else None,
    }


def _extract_tags(doc: Element) -> dict[str, str | None]:
    """Extract all Tag elements into a flat dict keyed by TagName."""
    tags: dict[str, str | None] = {}
    for tag in doc.iter("Tag"):
        name = tag.get("TagName")
        if name is not None:
            tags[name] = tag.get("TagValue")
    return tags


def _extract_document(doc: Element, custodian: str) -> dict[str, Any]:
    """Build a flat dict from a single <Document> element."""
    tags = _extract_tags(doc)
    files = _extract_files(doc)
    location = _extract_location(doc)

    record: dict[str, Any] = {
        "doc_id": doc.get("DocID"),
        "doc_type": doc.get("DocType"),
        "mime_type": doc.get("MimeType"),
        "custodian": custodian,
        **files,
        **location,
    }

    # Add all tags with cleaned-up key names
    for key, value in tags.items():
        clean_key = key.lstrip("#").lower().replace(" ", "_")
        record[clean_key] = value

    return record


def parse_xml_manifest(
    xml_path: Path,
    custodian: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Stream-parse an XML manifest, returning messages, files, and relationships.

    Returns:
        Tuple of (message_records, file_records, relationships) where
        relationships maps child_doc_id -> parent_doc_id.
    """
    messages: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    relationships: dict[str, str] = {}

    context = iterparse(str(xml_path), events=("end",))
    for _event, elem in context:
        if elem.tag == "Document":
            record = _extract_document(elem, custodian)
            if record["doc_type"] == "Message":
                messages.append(record)
            elif record["doc_type"] == "File":
                files.append(record)
            elem.clear()
        elif elem.tag == "Relationship":
            parent = elem.get("ParentDocId")
            child = elem.get("ChildDocId")
            if parent and child:
                relationships[child] = parent
            elem.clear()

    return messages, files, relationships


def _messages_to_dataframe(records: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert message records to a typed Polars DataFrame."""
    if not records:
        return _empty_messages_df()

    df = pl.DataFrame(records)

    # Select and cast columns in canonical order
    columns = {
        "doc_id": pl.Utf8,
        "custodian": pl.Utf8,
        "custodian_raw": pl.Utf8,
        "from": pl.Utf8,
        "to": pl.Utf8,
        "cc": pl.Utf8,
        "subject": pl.Utf8,
        "datesent": pl.Utf8,  # parsed to datetime below
        "hasattachments": pl.Utf8,
        "attachmentcount": pl.Utf8,
        "attachmentnames": pl.Utf8,
        "x-sdoc": pl.Utf8,
        "x-zlid": pl.Utf8,
        "location_uri": pl.Utf8,
        "native_path": pl.Utf8,
        "native_hash": pl.Utf8,
        "native_size": pl.Utf8,
        "text_path": pl.Utf8,
    }

    # Add any missing columns as null
    for col, dtype in columns.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    df = df.select(list(columns.keys()))

    # Type conversions
    df = df.with_columns(
        pl.col("datesent")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False)
        .alias("date_sent"),
        pl.col("hasattachments")
        .map_elements(lambda v: v == "true" if v else None, return_dtype=pl.Boolean)
        .alias("has_attachments"),
        pl.col("attachmentcount").cast(pl.UInt16, strict=False).alias("attachment_count"),
        pl.col("native_size").cast(pl.UInt32, strict=False).alias("native_file_size"),
    )

    return df.select(
        "doc_id",
        "custodian",
        "custodian_raw",
        pl.col("from").alias("from_"),
        "to",
        "cc",
        "subject",
        "date_sent",
        "has_attachments",
        "attachment_count",
        pl.col("attachmentnames").alias("attachment_names"),
        pl.col("x-sdoc").alias("x_sdoc"),
        pl.col("x-zlid").alias("x_zlid"),
        "location_uri",
        "native_path",
        "native_hash",
        "native_file_size",
        "text_path",
    )


def _attachments_to_dataframe(
    records: list[dict[str, Any]],
    relationships: dict[str, str],
) -> pl.DataFrame:
    """Convert file/attachment records to a typed Polars DataFrame."""
    if not records:
        return _empty_attachments_df()

    # Attach parent_doc_id from relationships
    for rec in records:
        rec["parent_doc_id"] = relationships.get(rec["doc_id"])

    df = pl.DataFrame(records)

    columns = {
        "doc_id": pl.Utf8,
        "parent_doc_id": pl.Utf8,
        "custodian": pl.Utf8,
        "filename": pl.Utf8,
        "fileextension": pl.Utf8,
        "filesize": pl.Utf8,
        "datecreated": pl.Utf8,
        "datemodified": pl.Utf8,
        "mime_type": pl.Utf8,
        "native_path": pl.Utf8,
        "native_hash": pl.Utf8,
        "text_path": pl.Utf8,
    }

    for col, dtype in columns.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    df = df.select(list(columns.keys()))

    df = df.with_columns(
        pl.col("filesize").cast(pl.UInt64, strict=False).alias("file_size"),
        pl.col("datecreated")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False)
        .alias("date_created"),
        pl.col("datemodified")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False)
        .alias("date_modified"),
    )

    return df.select(
        "doc_id",
        "parent_doc_id",
        "custodian",
        pl.col("filename").alias("file_name"),
        pl.col("fileextension").alias("file_extension"),
        "file_size",
        "date_created",
        "date_modified",
        "mime_type",
        "native_path",
        "native_hash",
        "text_path",
    )


def _empty_messages_df() -> pl.DataFrame:
    """Return an empty DataFrame with the messages schema."""
    return pl.DataFrame(
        schema={
            "doc_id": pl.Utf8,
            "custodian": pl.Utf8,
            "custodian_raw": pl.Utf8,
            "from_": pl.Utf8,
            "to": pl.Utf8,
            "cc": pl.Utf8,
            "subject": pl.Utf8,
            "date_sent": pl.Datetime("us", "UTC"),
            "has_attachments": pl.Boolean,
            "attachment_count": pl.UInt16,
            "attachment_names": pl.Utf8,
            "x_sdoc": pl.Utf8,
            "x_zlid": pl.Utf8,
            "location_uri": pl.Utf8,
            "native_path": pl.Utf8,
            "native_hash": pl.Utf8,
            "native_file_size": pl.UInt32,
            "text_path": pl.Utf8,
        }
    )


def _empty_attachments_df() -> pl.DataFrame:
    """Return an empty DataFrame with the attachments schema."""
    return pl.DataFrame(
        schema={
            "doc_id": pl.Utf8,
            "parent_doc_id": pl.Utf8,
            "custodian": pl.Utf8,
            "file_name": pl.Utf8,
            "file_extension": pl.Utf8,
            "file_size": pl.UInt64,
            "date_created": pl.Datetime("us", "UTC"),
            "date_modified": pl.Datetime("us", "UTC"),
            "mime_type": pl.Utf8,
            "native_path": pl.Utf8,
            "native_hash": pl.Utf8,
            "text_path": pl.Utf8,
        }
    )


def parse_custodian(custodian_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse all XML manifests in a custodian directory.

    Returns:
        Tuple of (messages_df, attachments_df).
    """
    custodian = custodian_dir.name
    all_messages: list[dict[str, Any]] = []
    all_files: list[dict[str, Any]] = []
    all_relationships: dict[str, str] = {}

    for xml_path in sorted(custodian_dir.glob("*.xml")):
        messages, files, relationships = parse_xml_manifest(xml_path, custodian)
        all_messages.extend(messages)
        all_files.extend(files)
        all_relationships.update(relationships)

    messages_df = _messages_to_dataframe(all_messages)
    attachments_df = _attachments_to_dataframe(all_files, all_relationships)
    return messages_df, attachments_df


def parse_all(unpacked_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse all custodian directories under unpacked_dir.

    Returns:
        Tuple of (messages_df, attachments_df) with all custodians combined.
    """
    msg_frames: list[pl.DataFrame] = []
    att_frames: list[pl.DataFrame] = []

    for custodian_dir in sorted(unpacked_dir.iterdir()):
        if not custodian_dir.is_dir():
            continue
        # Must have at least one XML file
        if not list(custodian_dir.glob("*.xml")):
            continue
        messages_df, attachments_df = parse_custodian(custodian_dir)
        msg_frames.append(messages_df)
        att_frames.append(attachments_df)

    messages = pl.concat(msg_frames) if msg_frames else _empty_messages_df()
    attachments = pl.concat(att_frames) if att_frames else _empty_attachments_df()
    return messages, attachments


def write_parquet(
    messages: pl.DataFrame,
    attachments: pl.DataFrame,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write messages and attachments DataFrames to Parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    msg_path = output_dir / "xml_messages.parquet"
    att_path = output_dir / "xml_attachments.parquet"
    messages.write_parquet(msg_path)
    attachments.write_parquet(att_path)
    return msg_path, att_path

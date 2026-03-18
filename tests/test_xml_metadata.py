"""Tests for XML metadata parsing."""

from pathlib import Path

import polars as pl
import pytest

from enron_emails.xml_metadata import (
    _attachments_to_dataframe,
    _empty_attachments_df,
    _empty_messages_df,
    _messages_to_dataframe,
    parse_all,
    parse_custodian,
    parse_xml_manifest,
    write_parquet,
)


class TestParseXmlManifest:
    """Tests for low-level XML parsing."""

    def test_returns_correct_counts(self, harris_xml: Path) -> None:
        messages, files, relationships = parse_xml_manifest(harris_xml, "harris-s")
        assert len(messages) == 611
        assert len(files) == 678
        assert len(relationships) == 678

    def test_message_has_expected_fields(self, harris_xml: Path) -> None:
        messages, _, _ = parse_xml_manifest(harris_xml, "harris-s")
        msg = messages[0]
        assert "doc_id" in msg
        assert "from" in msg
        assert "to" in msg
        assert "subject" in msg
        assert "datesent" in msg
        assert msg["custodian"] == "harris-s"
        assert msg["doc_type"] == "Message"

    def test_file_has_expected_fields(self, harris_xml: Path) -> None:
        _, files, _ = parse_xml_manifest(harris_xml, "harris-s")
        f = files[0]
        assert "doc_id" in f
        assert "filename" in f
        assert "fileextension" in f
        assert f["doc_type"] == "File"

    def test_relationships_link_child_to_parent(self, harris_xml: Path) -> None:
        _, files, relationships = parse_xml_manifest(harris_xml, "harris-s")
        for f in files:
            assert f["doc_id"] in relationships
            parent = relationships[f["doc_id"]]
            # Parent ID should be a prefix of child ID
            assert f["doc_id"].startswith(parent)

    def test_native_path_populated(self, harris_xml: Path) -> None:
        messages, _, _ = parse_xml_manifest(harris_xml, "harris-s")
        for msg in messages[:10]:
            assert msg["native_path"] is not None
            assert msg["native_path"].endswith(".eml")


class TestDataFrameConversion:
    """Tests for converting parsed records to Polars DataFrames."""

    def test_messages_dataframe_shape(self, harris_xml: Path) -> None:
        messages, _, _ = parse_xml_manifest(harris_xml, "harris-s")
        df = _messages_to_dataframe(messages)
        assert df.height == 611
        assert "doc_id" in df.columns
        assert "from_" in df.columns
        assert "date_sent" in df.columns

    def test_messages_date_parsed(self, harris_xml: Path) -> None:
        messages, _, _ = parse_xml_manifest(harris_xml, "harris-s")
        df = _messages_to_dataframe(messages)
        # date_sent should be datetime type
        assert df["date_sent"].dtype == pl.Datetime("us", "UTC")
        # Should have no nulls (all messages have dates)
        null_count = df["date_sent"].null_count()
        assert null_count == 0, f"Expected 0 null dates, got {null_count}"

    def test_messages_boolean_conversion(self, harris_xml: Path) -> None:
        messages, _, _ = parse_xml_manifest(harris_xml, "harris-s")
        df = _messages_to_dataframe(messages)
        assert df["has_attachments"].dtype == pl.Boolean

    def test_attachments_dataframe_shape(self, harris_xml: Path) -> None:
        _, files, relationships = parse_xml_manifest(harris_xml, "harris-s")
        df = _attachments_to_dataframe(files, relationships)
        assert df.height == 678
        assert "parent_doc_id" in df.columns
        assert "file_name" in df.columns

    def test_attachments_have_parent(self, harris_xml: Path) -> None:
        _, files, relationships = parse_xml_manifest(harris_xml, "harris-s")
        df = _attachments_to_dataframe(files, relationships)
        assert df["parent_doc_id"].null_count() == 0

    def test_empty_dataframes(self) -> None:
        msg_df = _empty_messages_df()
        att_df = _empty_attachments_df()
        assert msg_df.height == 0
        assert att_df.height == 0
        assert "doc_id" in msg_df.columns
        assert "doc_id" in att_df.columns


class TestParseCustodian:
    """Tests for custodian-level parsing."""

    def test_parse_harris(self, harris_dir: Path) -> None:
        messages, attachments = parse_custodian(harris_dir)
        assert messages.height == 611
        assert attachments.height == 678
        assert messages["custodian"][0] == "harris-s"


class TestParseAll:
    """Tests for parsing all custodians."""

    def test_parse_all_finds_harris(self, data_dir: Path) -> None:
        unpacked = data_dir / "unpacked"
        if not unpacked.exists():
            pytest.skip("unpacked data not available")
        messages, attachments = parse_all(unpacked)
        assert messages.height >= 611  # at least harris-s
        assert attachments.height >= 678


class TestWriteParquet:
    """Tests for Parquet output."""

    def test_roundtrip(self, harris_dir: Path, tmp_path: Path) -> None:
        messages, attachments = parse_custodian(harris_dir)
        msg_path, att_path = write_parquet(messages, attachments, tmp_path)

        assert msg_path.exists()
        assert att_path.exists()

        # Read back and verify
        msg_read = pl.read_parquet(msg_path)
        att_read = pl.read_parquet(att_path)
        assert msg_read.height == 611
        assert att_read.height == 678

    def test_duckdb_can_read(self, harris_dir: Path, tmp_path: Path) -> None:
        """Verify DuckDB can query the output Parquet files."""
        import duckdb

        messages, attachments = parse_custodian(harris_dir)
        msg_path, att_path = write_parquet(messages, attachments, tmp_path)

        result = duckdb.sql(f"SELECT count(*) FROM '{msg_path}'").fetchone()
        assert result is not None
        assert result[0] == 611

        result = duckdb.sql(f"SELECT count(*) FROM '{att_path}'").fetchone()
        assert result is not None
        assert result[0] == 678

"""Tests for the .eml parser."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from enron_emails.eml_parse import (
    clean_subject,
    parse_address,
    parse_address_list,
    parse_custodian_emls,
    parse_date,
    parse_eml,
    split_reply,
    strip_footers,
)

# ---------------------------------------------------------------------------
# Address parsing
# ---------------------------------------------------------------------------


class TestParseAddress:
    def test_empty(self) -> None:
        assert parse_address("") == (None, None)
        assert parse_address("   ") == (None, None)

    def test_standard_rfc2822(self) -> None:
        name, addr = parse_address('"Jeff Dasovich" <jdasovic@enron.com>')
        assert name == "Jeff Dasovich"
        assert addr == "jdasovic@enron.com"

    def test_name_angle_no_quotes(self) -> None:
        name, addr = parse_address("Jeff Dasovich <jdasovic@enron.com>")
        assert name == "Jeff Dasovich"
        assert addr == "jdasovic@enron.com"

    def test_angle_only(self) -> None:
        name, addr = parse_address("<jdasovic@enron.com>")
        assert name is None
        assert addr == "jdasovic@enron.com"

    def test_bare_email(self) -> None:
        name, addr = parse_address("jdasovic@enron.com")
        assert name is None
        assert addr == "jdasovic@enron.com"

    def test_bare_name(self) -> None:
        name, addr = parse_address("Jeff Dasovich")
        assert name == "Jeff Dasovich"
        assert addr is None

    def test_x500_dn(self) -> None:
        raw = "/O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC"
        name, addr = parse_address(raw)
        assert name == "JDASOVIC"
        assert addr is None

    def test_x500_in_angle_brackets(self) -> None:
        raw = "Dasovich, Jeff </O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC>"
        name, addr = parse_address(raw)
        assert name == "Dasovich, Jeff"
        assert addr == "/O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC"

    def test_exchange_name_fragments(self) -> None:
        name, addr = parse_address('<Harris>,"Steven" </O=ENRON/OU=NA/CN=RECIPIENTS/CN=SHARRIS1>')
        assert name == "Steven Harris"
        assert addr == "/O=ENRON/OU=NA/CN=RECIPIENTS/CN=SHARRIS1"

    def test_exchange_name_only(self) -> None:
        name, addr = parse_address("<Mangin>,<Emmanuel>")
        assert name is not None
        assert "Mangin" in name
        assert "Emmanuel" in name
        assert addr is None

    def test_imceanotes(self) -> None:
        raw = "IMCEANOTES-user+40domain+2Ecom+40ENRON@ENRON.com"
        name, addr = parse_address(raw)
        assert addr == "user@domain.com@ENRON"

    def test_imceanotes_with_display_name(self) -> None:
        raw = (
            '"Greg Rowe" '
            "<IMCEANOTES-gr42271+40csmail+2Ecorp+2Efedex+2Ecom+40ENRON@ENRON.com>"
        )
        name, addr = parse_address(raw)
        assert name == "Greg Rowe"
        assert addr is not None
        assert "gr42271@csmail.corp.fedex.com" in addr


class TestParseAddressList:
    def test_empty(self) -> None:
        assert parse_address_list("") == []
        assert parse_address_list("  ") == []

    def test_single(self) -> None:
        result = parse_address_list("jdasovic@enron.com")
        assert result == ["jdasovic@enron.com"]

    def test_comma_separated(self) -> None:
        result = parse_address_list("a@b.com, c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_semicolon_separated(self) -> None:
        result = parse_address_list("a@b.com; c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_preserves_angle_brackets(self) -> None:
        raw = '"Alice" <a@b.com>, "Bob" <c@d.com>'
        result = parse_address_list(raw)
        assert len(result) == 2
        assert "<a@b.com>" in result[0]


# ---------------------------------------------------------------------------
# Subject cleaning
# ---------------------------------------------------------------------------


class TestCleanSubject:
    def test_no_prefix(self) -> None:
        clean, reply, fwd = clean_subject("Meeting tomorrow")
        assert clean == "Meeting tomorrow"
        assert reply is False
        assert fwd is False

    def test_re(self) -> None:
        clean, reply, fwd = clean_subject("RE: Meeting tomorrow")
        assert clean == "Meeting tomorrow"
        assert reply is True
        assert fwd is False

    def test_fw(self) -> None:
        clean, reply, fwd = clean_subject("FW: Meeting tomorrow")
        assert clean == "Meeting tomorrow"
        assert reply is False
        assert fwd is True

    def test_fwd(self) -> None:
        clean, reply, fwd = clean_subject("Fwd: Meeting tomorrow")
        assert clean == "Meeting tomorrow"
        assert reply is False
        assert fwd is True

    def test_nested(self) -> None:
        clean, reply, fwd = clean_subject("RE: FW: RE: Meeting")
        assert clean == "Meeting"
        assert reply is True
        assert fwd is True

    def test_empty(self) -> None:
        clean, reply, fwd = clean_subject("")
        assert clean == ""


# ---------------------------------------------------------------------------
# Body extraction and cleaning
# ---------------------------------------------------------------------------

# Long string constants — ruff E501 is unavoidable for realistic test data.

EDRM_FOOTER = (  # noqa: E501
    "\n***********\n"
    "EDRM Enron Email Data Set has been produced in EML, PST and NSF format"
    " by ZL Technologies, Inc. This Data Set is licensed under a Creative"
    " Commons Attribution 3.0 United States License"
    ' <http://creativecommons.org/licenses/by/3.0/us/> . To provide'
    ' attribution, please cite to "ZL Technologies, Inc.'
    ' (http://www.zlti.com)."\n***********'
)

ENRON_DISCLAIMER = (
    "\n**********************************************************************\n"
    "This e-mail is the property of Enron Corp. and/or its relevant affiliate"
    " and may contain confidential and privileged material for the sole use of"
    " the intended recipient (s). Any review, use, distribution or disclosure"
    " by others is strictly prohibited.\n"
    "**********************************************************************"
)


class TestStripFooters:
    def test_strips_edrm_footer(self) -> None:
        body = "Hello world." + EDRM_FOOTER
        result = strip_footers(body)
        assert result == "Hello world."
        assert "EDRM" not in result

    def test_strips_enron_disclaimer(self) -> None:
        body = "Hello world." + ENRON_DISCLAIMER + EDRM_FOOTER
        result = strip_footers(body)
        assert "property of Enron" not in result
        assert "EDRM" not in result

    def test_preserves_body_without_footer(self) -> None:
        body = "Hello world.\nNo footer here."
        assert strip_footers(body) == body


class TestSplitReply:
    def test_no_reply(self) -> None:
        body = "Just a normal email."
        top, depth = split_reply(body)
        assert top == body
        assert depth == 0

    def test_original_message(self) -> None:
        body = (
            "My reply here.\n\n"
            " -----Original Message-----\n"
            "From: Someone\n"
            "Sent: Monday\n"
            "The original text."
        )
        top, depth = split_reply(body)
        assert top == "My reply here."
        assert depth == 1

    def test_forwarded(self) -> None:
        body = (
            "FYI.\n\n"
            "---------------------- Forwarded by Jeff/HOU/ECT on 01/06/2000\n"
            "Original content here."
        )
        top, depth = split_reply(body)
        assert top == "FYI."
        assert depth == 1

    def test_quote_block(self) -> None:
        body = "My reply.\n\n> line 1\n> line 2\n> line 3\n> line 4\n"
        top, depth = split_reply(body)
        assert top == "My reply."
        assert depth == 1

    def test_multiple_separators(self) -> None:
        body = (
            "Top.\n\n"
            " -----Original Message-----\n"
            "Middle.\n\n"
            " -----Original Message-----\n"
            "Bottom."
        )
        top, depth = split_reply(body)
        assert top == "Top."
        assert depth == 2


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_standard(self) -> None:
        dt = parse_date("Tue, 5 Sep 2000 07:31:00 -0700 (PDT)")
        assert dt is not None
        assert dt.year == 2000
        assert dt.month == 9

    def test_two_digit_year_normalised(self) -> None:
        # Python's email parser normalises year 0002 -> 2002
        dt = parse_date("Sun, 30 Nov 0002 00:00:00 -0800 (PST)")
        assert dt is not None
        assert dt.year == 2002

    def test_truly_bad_date(self) -> None:
        assert parse_date("not a date at all") is None

    def test_empty(self) -> None:
        assert parse_date("") is None
        assert parse_date(None) is None

    def test_utc_normalisation(self) -> None:
        dt = parse_date("Tue, 5 Sep 2000 07:31:00 -0700 (PDT)")
        assert dt is not None
        assert dt.utcoffset is not None
        # 07:31 PDT = 14:31 UTC
        assert dt.hour == 14


# ---------------------------------------------------------------------------
# Integration: parse a real .eml file
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"
HARRIS_DIR = DATA_DIR / "unpacked" / "harris-s"

# A known email with a reply chain
REPLY_EML = HARRIS_DIR / "native_000" / "3.287558.DHHYPMDBF0VJQVQB5QQTSSZIS5S0DAEFA.eml"
# A simple email
SIMPLE_EML = HARRIS_DIR / "native_000" / "3.287408.AOE4ILEUHOJYB2JN0ITXVSVP233T5AKRA.eml"


@pytest.mark.skipif(not REPLY_EML.exists(), reason="sample data not available")
class TestParseEmlIntegration:
    def test_reply_email(self) -> None:
        msg, atts = parse_eml(REPLY_EML, "harris-s")

        assert msg["doc_id"] == "3.287558.DHHYPMDBF0VJQVQB5QQTSSZIS5S0DAEFA"
        assert msg["custodian"] == "harris-s"
        assert msg["is_reply"] is True
        assert "EDRM" not in msg["body"]
        assert msg["reply_depth"] >= 1
        assert msg["body_top"]  # should have non-empty top portion
        assert len(msg["body_top"]) < len(msg["body"])

    def test_simple_email(self) -> None:
        msg, atts = parse_eml(SIMPLE_EML, "harris-s")

        assert msg["custodian"] == "harris-s"
        assert msg["date"] is not None
        assert "EDRM" not in msg["body"]
        # This email's Subject comes from X-ZL-Subject, not Subject header
        # so msg["subject"] may be empty -- that's correct behaviour


@pytest.mark.skipif(not HARRIS_DIR.exists(), reason="sample data not available")
class TestParseCustodianIntegration:
    def test_harris_s(self) -> None:
        messages_df, attachments_df = parse_custodian_emls(HARRIS_DIR)

        assert messages_df.height > 0
        assert "doc_id" in messages_df.columns
        assert "body" in messages_df.columns
        assert "body_top" in messages_df.columns

        # Spot check: no EDRM footer in any body
        bodies_with_edrm = messages_df.filter(
            pl.col("body").str.contains("EDRM Enron Email Data Set")
        )
        assert bodies_with_edrm.height == 0

        # Attachments table should have the right columns
        assert "parent_doc_id" in attachments_df.columns
        assert "mime_type" in attachments_df.columns

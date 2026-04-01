"""CLI entry points for enron-emails pipeline."""

import argparse
import sys
from pathlib import Path

from enron_emails.download import ensure_custodian
from enron_emails.eml_parse import (
    parse_all_emls,
    parse_custodian_emls,
    write_eml_parquet,
)
from enron_emails.xml_metadata import parse_all, parse_custodian, write_parquet


def _default_data_dir() -> Path:
    return Path("data")


def cmd_download(args: argparse.Namespace) -> None:
    """Download and unpack custodian archives."""
    data_dir = Path(args.data_dir)
    for custodian in args.custodians:
        print(f"Ensuring {custodian}...")
        path = ensure_custodian(custodian, data_dir)
        print(f"  -> {path}")


def _parse_xml(unpacked_dir: Path, output_dir: Path, custodians: list[str]) -> None:
    """Run the XML manifest parser."""
    print("--- XML manifests ---")
    if custodians:
        import polars as pl

        msg_frames = []
        att_frames = []
        for name in custodians:
            cdir = unpacked_dir / name
            if not cdir.exists():
                print(f"Warning: {cdir} not found, skipping", file=sys.stderr)
                continue
            print(f"Parsing {name}...")
            msgs, atts = parse_custodian(cdir)
            msg_frames.append(msgs)
            att_frames.append(atts)

        if not msg_frames:
            print("No XML data parsed.", file=sys.stderr)
            return

        messages = pl.concat(msg_frames)
        attachments = pl.concat(att_frames)
    else:
        print(f"Parsing all custodians in {unpacked_dir}...")
        messages, attachments = parse_all(unpacked_dir)

    msg_path, att_path = write_parquet(messages, attachments, output_dir)
    print(f"Messages:    {messages.height:>8,} rows -> {msg_path}")
    print(f"Attachments: {attachments.height:>8,} rows -> {att_path}")


def _parse_eml(unpacked_dir: Path, output_dir: Path, custodians: list[str]) -> None:
    """Run the .eml file parser."""
    print("--- EML files ---")
    if custodians:
        import polars as pl

        msg_frames = []
        att_frames = []
        for name in custodians:
            cdir = unpacked_dir / name
            if not cdir.exists():
                print(f"Warning: {cdir} not found, skipping", file=sys.stderr)
                continue
            print(f"Parsing {name}...")
            msgs, atts = parse_custodian_emls(cdir)
            print(f"  {msgs.height:,} messages, {atts.height:,} attachments")
            msg_frames.append(msgs)
            att_frames.append(atts)

        if not msg_frames:
            print("No EML data parsed.", file=sys.stderr)
            return

        messages = pl.concat(msg_frames)
        attachments = pl.concat(att_frames)
    else:
        print(f"Parsing all custodians in {unpacked_dir}...")
        messages, attachments = parse_all_emls(unpacked_dir)

    msg_path, att_path = write_eml_parquet(messages, attachments, output_dir)
    print(f"Messages:    {messages.height:>8,} rows -> {msg_path}")
    print(f"Attachments: {attachments.height:>8,} rows -> {att_path}")


def cmd_parse_xml(args: argparse.Namespace) -> None:
    """Parse XML manifests into Parquet."""
    data_dir = Path(args.data_dir)
    _parse_xml(data_dir / "unpacked", data_dir / "parquet", args.custodians or [])


def cmd_parse_eml(args: argparse.Namespace) -> None:
    """Parse .eml files into Parquet."""
    data_dir = Path(args.data_dir)
    _parse_eml(data_dir / "unpacked", data_dir / "parquet", args.custodians or [])


def cmd_embed(args: argparse.Namespace) -> None:
    """Generate OpenAI embeddings for email texts."""
    from enron_emails.embed import build_chunks, embed_all

    data_dir = Path(args.data_dir)
    input_path = data_dir / "parquet" / "eml_messages.parquet"
    output_dir = data_dir / "parquet" / "eml_embeddings"

    if not input_path.exists():
        print(f"Error: {input_path} not found. Run parse-eml first.", file=sys.stderr)
        sys.exit(1)

    custodians = args.custodians or None

    columns: list[str] = []
    if not args.no_body_top:
        columns.append("body_top")
    if args.body:
        columns.append("body")
    if not columns:
        columns = ["body_top"]

    for column in columns:
        print(f"--- Embedding {column} ---")
        result = embed_all(input_path, output_dir, column=column, custodians=custodians)
        print(f"  -> {result}")

    if args.chunks:
        print("\n--- Building chunked embeddings ---")
        chunks_path = build_chunks(input_path, data_dir / "parquet" / "eml_chunks.parquet")
        print(f"  -> {chunks_path}")


def cmd_upload(args: argparse.Namespace) -> None:
    """Upload Parquet files to S3-compatible storage."""
    from enron_emails.upload import upload_parquet

    data_dir = Path(args.data_dir)
    print("Uploading parquet files to S3...")
    uploaded = upload_parquet(data_dir)
    print(f"Uploaded {len(uploaded)} files.")


def cmd_pipeline(args: argparse.Namespace) -> None:
    """Download, unpack, and parse in one step."""
    cmd_download(args)
    cmd_parse_xml(args)
    print()
    cmd_parse_eml(args)


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="enron-emails",
        description="Preprocess the Enron email corpus into Parquet",
    )
    parser.add_argument(
        "--data-dir",
        default=str(_default_data_dir()),
        help="Root data directory (default: data/)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    dl = sub.add_parser("download", help="Download and unpack custodian archives")
    dl.add_argument("custodians", nargs="+", help="Custodian names (e.g. harris-s)")
    dl.set_defaults(func=cmd_download)

    parse_xml = sub.add_parser("parse-xml", help="Parse XML manifests to Parquet")
    parse_xml.add_argument("custodians", nargs="*", help="Custodian names (all if omitted)")
    parse_xml.set_defaults(func=cmd_parse_xml)

    parse_eml = sub.add_parser("parse-eml", help="Parse .eml files to Parquet")
    parse_eml.add_argument("custodians", nargs="*", help="Custodian names (all if omitted)")
    parse_eml.set_defaults(func=cmd_parse_eml)

    embed = sub.add_parser("embed", help="Generate OpenAI embeddings")
    embed.add_argument("custodians", nargs="*", help="Custodian names (all if omitted)")
    embed.add_argument("--body", action="store_true", help="Also embed full body")
    embed.add_argument("--no-body-top", action="store_true", help="Skip body_top embeddings")
    embed.add_argument("--chunks", action="store_true", help="Build chunked embeddings for long emails")
    embed.set_defaults(func=cmd_embed)

    upload = sub.add_parser("upload", help="Upload Parquet files to S3")
    upload.set_defaults(func=cmd_upload)

    pipe = sub.add_parser("pipeline", help="Download + parse in one step")
    pipe.add_argument("custodians", nargs="+", help="Custodian names")
    pipe.set_defaults(func=cmd_pipeline)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

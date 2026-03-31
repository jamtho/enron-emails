"""Download and unpack EDRM Enron email archive files."""

import urllib.request
import zipfile
from pathlib import Path

ARCHIVE_BASE_URL = "https://archive.org/download/edrm.enron.email.data.set.v2.xml"


def zip_url(custodian: str) -> str:
    """Build the archive.org download URL for a custodian zip."""
    return f"{ARCHIVE_BASE_URL}/edrm-enron-v2_{custodian}_xml.zip"


def zip_filename(custodian: str) -> str:
    """Return the expected zip filename for a custodian."""
    return f"edrm-enron-v2_{custodian}_xml.zip"


def download_zip(custodian: str, downloads_dir: Path) -> Path:
    """Download a custodian zip to downloads_dir. Skips if already present."""
    downloads_dir.mkdir(parents=True, exist_ok=True)
    dest = downloads_dir / zip_filename(custodian)
    if dest.exists():
        return dest
    url = zip_url(custodian)
    urllib.request.urlretrieve(url, dest)  # noqa: S310
    return dest


def custodian_from_zip(zip_path: Path) -> str:
    """Derive custodian name from a zip filename.

    Handles both single-file and multi-part naming:
      edrm-enron-v2_harris-s_xml.zip       -> harris-s
      edrm-enron-v2_kaminski-v_xml_1of2.zip -> kaminski-v
      edrm-enron-v2_kean-s_xml_3of8.zip    -> kean-s
    """
    stem = zip_path.stem  # e.g. edrm-enron-v2_harris-s_xml or ..._kaminski-v_xml_1of2
    parts = stem.split("_")
    return parts[1] if len(parts) >= 3 else stem


def unpack_zip(zip_path: Path, unpacked_dir: Path) -> Path:
    """Extract a custodian zip into unpacked_dir/{custodian}/.

    Multi-part zips for the same custodian are merged into one directory.
    Single-part zips are skipped if already unpacked.
    """
    custodian = custodian_from_zip(zip_path)
    stem = zip_path.stem
    is_multipart = any(c.isdigit() for c in stem.split("_")[-1]) and "of" in stem

    dest = unpacked_dir / custodian

    # For single-part zips, skip if already unpacked
    if not is_multipart and dest.exists() and any(dest.glob("*.xml")):
        return dest

    # For multi-part zips, use a marker file to track which parts are done
    if is_multipart:
        marker = dest / f".unpacked_{stem}"
        if marker.exists():
            return dest

    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)

    if is_multipart:
        (dest / f".unpacked_{stem}").touch()

    return dest


def ensure_custodian(custodian: str, data_dir: Path) -> Path:
    """Download and unpack a custodian archive. Returns the unpacked directory.

    Handles multi-part zips (e.g. kaminski-v_xml_1of2.zip, _2of2.zip) by
    discovering all parts in the downloads directory.
    """
    downloads_dir = data_dir / "downloads"
    unpacked_dir = data_dir / "unpacked"

    # Check for multi-part zips first
    pattern = f"edrm-enron-v2_{custodian}_xml_*of*.zip"
    multipart = sorted(downloads_dir.glob(pattern))
    if multipart:
        dest = unpacked_dir / custodian
        for part in multipart:
            dest = unpack_zip(part, unpacked_dir)
        return dest

    # Single zip — download if needed, then unpack
    zip_path = download_zip(custodian, downloads_dir)
    return unpack_zip(zip_path, unpacked_dir)

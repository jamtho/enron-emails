"""Shared test fixtures."""

from pathlib import Path

import pytest

DATA_DIR = Path(__file__).parent.parent / "data"
HARRIS_DIR = DATA_DIR / "unpacked" / "harris-s"
HARRIS_XML = HARRIS_DIR / "zl_harris-s_692_NOFN_000.xml"


@pytest.fixture
def harris_dir() -> Path:
    """Path to the unpacked harris-s sample data."""
    if not HARRIS_DIR.exists():
        pytest.skip("harris-s sample data not available")
    return HARRIS_DIR


@pytest.fixture
def harris_xml() -> Path:
    """Path to the harris-s XML manifest."""
    if not HARRIS_XML.exists():
        pytest.skip("harris-s XML manifest not available")
    return HARRIS_XML


@pytest.fixture
def data_dir() -> Path:
    """Path to the data directory."""
    return DATA_DIR

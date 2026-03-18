"""Smoke tests to verify the environment is set up correctly."""


def test_import() -> None:
    import enron_emails  # noqa: F811

    assert enron_emails.__doc__ is not None


def test_polars_available() -> None:
    import polars as pl

    df = pl.DataFrame({"a": [1, 2, 3]})
    assert df.shape == (3, 1)


def test_duckdb_available() -> None:
    import duckdb

    result = duckdb.sql("SELECT 42 AS answer").fetchone()
    assert result is not None
    assert result[0] == 42

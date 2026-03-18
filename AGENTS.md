# Agents

## Code style

- Use type hints extensively on all function signatures and non-trivial variables.
- Target Python 3.12+; use modern syntax (`type` statements, `X | Y` unions, etc.).
- Follow ruff defaults plus the lint rules configured in `pyproject.toml`.

## Libraries

- **Polars** for in-process data frame operations — avoid pandas.
- **DuckDB** for SQL-based exploration, heavy aggregations, and Parquet I/O.
- **PyArrow** as the shared interchange format between DuckDB and Polars.
- Use **uv** for dependency management and virtual environments (`uv sync`, `uv run`).

## Testing

- Write good, focused tests for every module and function.
- Run the full test suite (`uv run pytest`) after every change — do not commit code that fails tests.
- Prefer real data fixtures (small sample `.eml` files or raw text) over mocks where practical.
- Use `pytest` parametrize for edge cases.

## Commits

- Commit regularly in small, logical increments.
- Write clear, conventional commit messages (e.g. `feat:`, `fix:`, `test:`, `refactor:`).
- Do not include "with Claude", "AI-generated", co-author tags, or similar disclaimers in commit messages or code.

## Project conventions

- Keep the `src/enron_emails/` package as the single source of library code.
- Put CLI entry points in `src/enron_emails/cli.py`.
- Data files (raw dump, intermediates, output parquet) go in `data/` which is git-ignored.
- Prefer lazy evaluation and streaming where possible — the full corpus is large.
- Document non-obvious design decisions in docstrings, but don't over-comment obvious code.

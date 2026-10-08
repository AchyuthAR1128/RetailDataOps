# RetailDataOps

**Orchestrated Retail Data Engineering & Analytics Platform**

[![CI](https://github.com/<owner>/RetailDataOps/actions/workflows/ci.yml/badge.svg)](https://github.com/<owner>/RetailDataOps/actions/workflows/ci.yml)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Linting: ruff](https://img.shields.io/badge/linting-ruff-261230.svg)](https://github.com/astral-sh/ruff)

---

## Overview

RetailDataOps is a production-style data engineering portfolio project
demonstrating end-to-end retail analytics — from raw data ingestion through
medallion-architecture transformations to forecasting and dashboards.

> **Status:** Phase 1 — Repository structure, tooling, CI skeleton, and secrets
> handling.

## Quickstart

```bash
# Clone the repository
git clone https://github.com/<owner>/RetailDataOps.git
cd RetailDataOps

# Create and activate virtual environment (Python 3.10)
python3 -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Run all checks
make check    # or manually: ruff check . && mypy src/ && pytest
```

## Project Structure

```
RetailDataOps/
├── .github/workflows/    # CI/CD pipeline definitions
├── src/retaildataops/    # Core Python package
├── tests/                # Test suite
├── scripts/              # Developer utility scripts
├── docs/                 # Documentation
├── pyproject.toml        # Project config (deps, tools)
├── Makefile              # Task runner
└── .pre-commit-config.yaml
```

## Development Commands

| Command          | Description                         |
| ---------------- | ----------------------------------- |
| `make install`   | Install package + dev dependencies  |
| `make lint`      | Run ruff linter                     |
| `make format`    | Run black formatter                 |
| `make typecheck` | Run mypy type checker               |
| `make test`      | Run pytest with coverage            |
| `make check`     | Run all checks (lint + type + test) |
| `make clean`     | Remove build/cache artifacts        |

## Secrets Handling

- Copy `.env.example` → `.env` and fill in real values.
- `.env` is **gitignored** — never committed.
- CI uses GitHub Actions encrypted secrets.
- See `.env.example` for all required variables.

## License

[MIT](LICENSE)

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

> **Status:** Phase 4 — CI/CD and deployment readiness for the local
> Replay-to-Gold pipeline.

## Prerequisites

- Python 3.10.x
- Java 17
- Git
- Linux, WSL2 Ubuntu, or another Linux environment for Spark and Delta tests
- The M5 source files listed in [Dataset setup](#dataset-setup)

Native Windows Spark filesystem writes are not supported by this project's
validated local setup. On Windows, use WSL2 Ubuntu with Java 17.

## Installation

```bash
# Clone the repository
git clone https://github.com/<owner>/RetailDataOps.git
cd RetailDataOps

# Create and activate a Python 3.10 virtual environment
python3.10 -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
pre-commit install
```

For WSL2 Ubuntu, install and select Java 17 with:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

The validated Spark dependencies are PySpark 3.5.9 and Delta Lake 3.3.3.
The CI workflow installs those exact versions.

## Dataset setup

Place these files under `data/raw/`:

```text
data/raw/
├── calendar.csv
├── sales_train_validation.csv
└── sell_prices.csv
```

The raw dataset and generated landing/Bronze/Silver/Gold outputs are ignored
by Git. Do not commit source data, credentials, or generated Delta tables.

## Quality checks

Run the checks used by CI, plus the local pre-commit hooks:

```bash
ruff check .
black --check .
mypy src/
SPARK_LOCAL_IP=127.0.0.1 pytest
pre-commit run --all-files
```

Or use the Makefile targets:

```bash
make install
make check
```

`pytest` runs the unit, Delta integration, orchestration, and synthetic
end-to-end tests with coverage.

## Running the pipeline

Run Replay through Gold for a date range:

```bash
SPARK_LOCAL_IP=127.0.0.1 python scripts/run_pipeline.py \
  --start-date 2011-01-29 \
  --end-date 2011-02-04
```

Run an explicitly selected deterministic batch:

```bash
SPARK_LOCAL_IP=127.0.0.1 python scripts/run_pipeline.py \
  --batch-id batch_20110129_20110204
```

Override locations and allow one retry per failed stage:

```bash
SPARK_LOCAL_IP=127.0.0.1 python scripts/run_pipeline.py \
  --batch-id batch_20110129_20110204 \
  --raw-dir data/raw \
  --landing-dir data/landing \
  --bronze-dir data/bronze \
  --silver-dir data/silver \
  --gold-dir data/gold \
  --retries 1
```

The orchestrator runs:

```text
Replay -> Bronze -> Silver -> Gold
```

It stops on stage failure, retries only the failed stage when requested, and
preserves the existing idempotency and conflict detection behavior.

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

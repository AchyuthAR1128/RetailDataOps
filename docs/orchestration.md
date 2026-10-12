# RetailDataOps Pipeline Orchestration

`scripts/run_pipeline.py` composes the existing stage runners in this order:

```text
Replay -> Bronze -> Silver -> Gold
```

The orchestrator does not duplicate transformations. It invokes each existing
CLI runner as a child process, forwards the configured paths, and stops at the
first non-zero stage result. Each stage logs start, success, and failure.

## Run a date range

From the repository root:

```bash
python scripts/run_pipeline.py \
  --start-date 2011-01-29 \
  --end-date 2011-02-04
```

The batch ID is deterministic (`batch_20110129_20110204`) and is forwarded to
Bronze and Silver. Gold reads the complete accumulated Silver table so that
analytical outputs remain complete across multiple batches.

## Select an existing batch explicitly

The batch ID can be used instead of dates:

```bash
python scripts/run_pipeline.py \
  --batch-id batch_20110129_20110204
```

If both forms are supplied, they must describe the same batch.

## Configure paths and retries

```bash
python scripts/run_pipeline.py \
  --batch-id batch_20110129_20110204 \
  --raw-dir data/raw \
  --landing-dir data/landing \
  --bronze-dir data/bronze \
  --silver-dir data/silver \
  --gold-dir data/gold \
  --retries 1
```

Retries repeat only the failed stage. They never delete existing data or
disable validation. Replay, Bronze, Silver, and Gold retain their existing
idempotency and conflict detection, so a conflicting retry fails explicitly.

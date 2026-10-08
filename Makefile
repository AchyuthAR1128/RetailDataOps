# =============================================================================
# RetailDataOps — Makefile
# =============================================================================
# Usage:  make <target>
#
# On Windows without Make, run the commands directly in PowerShell.
# =============================================================================

.DEFAULT_GOAL := help

.PHONY: help install lint format typecheck test check clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install package in editable mode with dev dependencies
	pip install -e ".[dev]"
	pre-commit install

lint: ## Run ruff linter
	ruff check .

format: ## Run black formatter
	black .

typecheck: ## Run mypy type checker
	mypy src/

test: ## Run pytest with coverage
	pytest

check: lint typecheck test ## Run all checks (lint + typecheck + test)
	@echo "All checks passed."

clean: ## Remove build/cache artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

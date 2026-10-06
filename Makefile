.PHONY: lint test test-unit test-fuzz test-integration mutation

lint:
	uv run ruff check src tests main.py

test: test-unit test-fuzz

test-unit:
	uv run pytest tests/unit -v

test-fuzz:
	uv run pytest tests/fuzz -v

# Requer Docker funcionando no host
test-integration:
	uv run pytest tests/integration -v

mutation:
	uv run mutmut run

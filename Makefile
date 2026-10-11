.PHONY: lint test

lint:
	uv run ruff check src tests flows pipelines

# Spark in Docker (testcontainers; requires Docker on the host).
test:
	uv run pytest tests -v

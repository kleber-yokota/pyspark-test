.PHONY: lint test

lint:
	uv run ruff check src tests main.py

# Padrao: Spark em Docker (testcontainers; requer Docker no host).
# Sem Docker: SPARK_LOCAL=1 make test (requer JVM).
test:
	uv run pytest tests -v

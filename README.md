# pyspark-test

Teste unitario de uma transformacao **PySpark** (Spark 4.x), com Spark em
Docker via **testcontainers**: uma funcao recebe um DataFrame com a coluna
``name``, aplica ``initcap`` e cria a nova coluna ``name_cap``.

## O codigo

- **`src/pyspark_test/text.py`** — `capitalize_names(df)`: cria a coluna
  `name_cap` aplicando `F.initcap("name")` (`"maria da silva" -> "Maria Da
  Silva"`). A coluna original e mantida.
- **`main.py`** — demo: roda a funcao em um DataFrame de exemplo (Spark local).

## Testes

- **`tests/test_capitalize.py`** — suite contra um Spark real: cria um
  DataFrame com a coluna `name`, aplica `capitalize_names` e confere a nova
  coluna `name_cap`.
- **`tests/conftest.py`** — fixture `spark` (scope `session`):
  - padrao: sobe o servidor **Spark Connect** no container
    `apache/spark:4.2.0-python3` via **testcontainers** (`DockerContainer` +
    `PortWaitStrategy`);
  - `SPARK_LOCAL=1`: Spark em modo local no host (requer JVM) — para
    desenvolver sem Docker.

## Como rodar

```bash
uv sync
make lint                                # ruff
make test                                # Spark em Docker (testcontainers)
SPARK_LOCAL=1 make test                  # Spark local (sem Docker, requer JVM)
```

Variaveis uteis:

- `SPARK_IMAGE` — image do Spark (padrao `apache/spark:4.2.0-python3`;
  mantenha a **mesma versao** do `pyspark` instalado).
- `SPARK_STARTUP_TIMEOUT_S` — timeout do wait strategy (padrao 240s).
- `SPARK_LOCAL=1` — usa Spark local em vez do container (requer JVM no host).

## CI (`.github/workflows/ci.yml`)

1. **lint** — `ruff check`
2. **test** — `pytest tests` com o container Spark + cobertura
   (`--cov-fail-under=90`)

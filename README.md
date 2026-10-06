# pyspark-test

Pipeline PySpark com **camadas de teste** — cada camada valida uma coisa
diferente e roda em velocidades diferentes:

| Camada | O que testa | Infra | Velocidade |
|---|---|---|---|
| **Unit** (`tests/unit`) | Lógica pura (`rows.py`) — parse/validação | nada | segundos |
| **Fuzz** (`tests/fuzz`) | Propriedades da lógica pura (Hypothesis, entradas aleatórias) | nada | segundos |
| **Mutação** (`mutmut`) | Se a suite unitária realmente mata mutantes da lógica pura | nada | minutos |
| **Integração** (`tests/integration`) | Colagem com Spark real (DataFrame, group by, Spark Connect) | Docker + testcontainers | minutos (container sobe 1x por sessão) |

## Arquitetura de teste

- **`src/pyspark_test/rows.py`** — lógica pura, sem Spark. É o alvo de unit/fuzz/mutação:
  buga com mais frequência e precisa ser testada rápido.
- **`src/pyspark_test/pipeline.py`** — colagem Spark (load → filtro → agregação).
  É fina de propósito; a lógica pesada fica em `rows.py`.
- **`tests/conftest.py`** — fixture `spark` (scope `session`) que sobe um servidor
  **Spark Connect** dentro do container `apache/spark` via **testcontainers**
  (`DockerContainer` + `PortWaitStrategy`). A fixture só é acionada quando um
  teste a declara como dependência — a suite unitária/fuzz não paga esse custo.

> **Por que Spark Connect em vez de PySpark local?**
> Não existe módulo oficial `SparkContainer` no testcontainers-python, e o
> PySpark clássico exige JVM no host. Com Spark Connect, o lado do teste é um
> cliente gRPC puro em Python (`pip install pyspark[connect]`) e todo o Spark
> roda dentro do container — mesma versão da produção, sem Java no host.

## Como rodar

```bash
uv sync                                  # instala tudo
make test                                # unit + fuzz (sem Docker)
make test-integration                    # Spark em Docker (requer Docker no host)
make mutation                            # mutmut sobre rows.py
make lint                                # ruff
```

Variáveis úteis:

- `SPARK_IMAGE` — image do Spark (padrão `apache/spark:4.2.0-python3`; mantenha a
  **mesma versão** do `pyspark` instalado).
- `HYPOTHESIS_PROFILE=fast` — menos exemplos do Hypothesis (padrão `ci`).
- `SPARK_STARTUP_TIMEOUT_S` — timeout do wait strategy (padrão 240s).

## CI (`.github/workflows/ci.yml`)

Ordem dos jobs (dependências):

1. **lint** — `ruff check`
2. **unit-and-fuzz** — `pytest tests/unit tests/fuzz` + cobertura da camada pura (`--cov-fail-under=95`)
3. **mutation** — `mutmut run` (falha se sobreviver mutante) + artifact `mutmut.html`
4. **integration** — `pytest tests/integration` com o container Spark + cobertura da colagem (`--cov-fail-under=90`)

O runner do GitHub Actions já tem Docker; o job de integração só precisa do
`docker pull` (feito no workflow).

## Adicionando testes

- **Nova regra de negócio** → adicione a lógica pura em `rows.py`, unit test em
  `tests/unit/`, propriedade de invariantes em `tests/fuzz/` e deixe o mutmut
  cobrir (ele já aponta só para `rows.py` no `[tool.mutmut]`).
- **Nova etapa de Spark** (ex.: join, write em S3) → função em `pipeline.py` +
  teste em `tests/integration/` usando a fixture `spark`.
- **Novo serviço** (ex.: MinIO/S3 pro storage) → container em `tests/conftest.py`
  com `Network` compartilhada e `network_aliases` (nunca `localhost` entre containers).

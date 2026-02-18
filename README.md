   ____ _ _   _ _   _ _   _ _ _ _ _   _   _  __ _  _   _  _   _ ___ _  _ 
  / ___(_) |_| | | | | | | | ____| \ | | / _| || | | || | | | |_ _| \| |
 | |  _| | __| | | | | | |  _| |  \| | | |_| || |_| || |_| | | || .` |
 | |_| | | |_| |_| |_| |_| |___| |\  | |  _|__   _|__   _|__   _| |_|_|
  \____|_|\__|\___/ \___/|_____|_| \_| |_|     |_|    |_|   |_|  (_)

GitHub Events Real-Time Streaming Analytics

Badges
- ![Python](https://img.shields.io/badge/python-3.11%2B-blue)
- ![License](https://img.shields.io/badge/license-MIT-green)
- ![Build Status](https://img.shields.io/badge/build-passing-brightgreen)

Resumo
-----
Pipeline de streaming que ingere GitHub Events, publica em Kafka, processa com Spark Structured Streaming e persiste métricas em Parquet para análises e dashboards.

Arquitetura (Mermaid)
---------------------
```mermaid
graph LR
  A[GitHub Events API] --> B[Producer Service]
  B -->|Kafka Topics per Event Type| C[Kafka Cluster]
  C --> D[Spark Structured Streaming]
  D --> E[Parquet (S3 / local)]
  E --> F[Analytics / Dashboards]
  B --> G[DLQ Topic]
  subgraph Observability
    H[Prometheus] & I[OpenTelemetry Collector]
  end
  B --> H
  D --> I
```

Quick start (local)
-------------------
Prereqs: Docker (or Docker Desktop), `docker compose` v2+, Python 3.11+, git.

1. Install dependencies (virtualenv optional)
```bash
# POSIX/MacOS
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Start local infra (Kafka + Zookeeper). Spark is opt-in via a compose profile.
```bash
# Start core infra (Kafka + Zookeeper)
docker compose up -d

# Start Spark (optional, slower and large image):
docker compose --profile spark up -d

# verify status
docker compose ps
```

3. Run producer locally (configured via env or `.env`)
```bash
# POSIX
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python -m src.producer.github_producer

# Windows (PowerShell)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python -m src.producer.github_producer
```

4. Run Spark consumer (when Spark service is available)
```bash
# If you started Spark via profile above
python -m src.consumer.spark_consumer
```

5. Analytics & dashboards
- Load Parquet and generate reports with `src/analytics/metrics.py`.
- Notebook example: [notebooks/exploratory_analysis.ipynb](notebooks/exploratory_analysis.ipynb)

Exemplos de uso (comandos & screenshots)
----------------------------------------
- executar um batch de ingestão:
```bash
python -m src.producer.github_producer --once
```
- executar Spark local:
```bash
python -m src.consumer.spark_consumer
```

Screenshots (exemplos)
- Producer logs (ex.: docs/screenshots/producer-cli.png) — saída JSON estruturado
- Spark UI (ex.: docs/screenshots/spark-ui.png) — job de streaming ativo
- Dashboard Plotly (ex.: docs/screenshots/dashboard.png)

(Coloque imagens em docs/screenshots/ com nomes acima para referencia)

Configuração
-------------
Centralizada via `src/config.py` (`config.AppConfig`). Exemplo .env:
```
GITHUB_TOKEN=
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PREFIX=github_events
METRICS_ENABLED=false
```

Observability & Tracing
-----------------------
- tracing: `src/tracing.py` (`tracing.setup_tracer`) (OTLP) — configure `OTEL_EXPORTER_OTLP_ENDPOINT`
- metrics: prometheus client expõe métricas quando `METRICS_ENABLED=true`

Métricas esperadas / Benchmarks
-------------------------------
Expectativas (ordem de grandeza):
- Latência de publicação Kafka (p99) < 100 ms
- Throughput por instância producer: 500–2000 events/s dependendo da serialização (JSON vs Avro) e rede

Cálculo rápido:
$E_{hour} = r \\times 3600$

Exemplo:
$$E_{hour} = 1000 \\times 3600 = 3.6 \\times 10^{6}$$

Troubleshooting
---------------
- Kafka unreachable: verifique `KAFKA_BOOTSTRAP_SERVERS` e `docker-compose ps`; logs em `docker-compose logs kafka`.
- Rate limit GitHub: parser no `src/producer/github_producer.fetch_events` já respeita X-RateLimit; use `GITHUB_TOKEN` para limites maiores.
- Falha na serialização Avro: confirme schemas em `AVRO_SCHEMA_DIR` e `USE_AVRO=true`.
- Tests falhando: execute `pytest -q` (tests: `tests/test_github_producer.py`).
- OpenTelemetry não ativa: variável `OTEL_EXPORTER_OTLP_ENDPOINT` ausente — tracing é opcional e tem fallback seguro (ver `src/tracing.py`).

Contribuindo
------------
1. Fork -> branch feature/descrição -> PR
2. Formatação: Black, lint com Flake8, tipagem com MyPy
```bash
black .
flake8
mypy src
pytest -q
```
Arquivos chave:
- produtor: `src/producer/github_producer.py`
- config: `src/config.py`
- consumer/spark: `src/consumer/spark_consumer.py`
- analytics: `src/analytics/metrics.py`

Guidelines
- testes unitários para novas funcionalidades
- evitar que secrets sejam commitados (.gitignore presente)
- documentar mudanças no README e no `infra/terraform/README.md`

Performance tuning pointers
--------------------------
- Prefira Avro + fastavro para payloads grandes (menor CPU / menor banda).
- Aumente `PRODUCER_MAX_IN_FLIGHT` e `linger_ms` conforme latência de rede.
- Scale horizontal do producer em múltiplas réplicas para throughput linear.

Roadmap
-------
- [ ] TLS/SASL para Kafka (MSK-ready)
- [ ] Schema Registry + Confluent/Glue integration
- [ ] Exactly-once semantics end-to-end (idempotent producer, transactional writes)
- [ ] Enhanced language detection (git metadata + GitHub GraphQL)
- [ ] Enrichment (geo resolution, user timezone inference)
- [ ] Real-time dashboards com auth & RBAC
- [ ] CI: GitHub Actions pipeline (lint/test/build badges)

Licença
-------
MIT (adapte conforme necessário)

Referências no repositório
-------------------------
- Config: `src/config.py` (`src/config.py`)
- Producer principal: `src/producer/github_producer.GitHubProducer` (`src/producer/github_producer.py`)
- Spark consumer: `src/consumer/spark_consumer.compute_metrics` (`src/consumer/spark_consumer.py`)
- Analytics helpers: `src/analytics/metrics.load_parquet` (`src/analytics/metrics.py`)
- Tracing helpers: `src/tracing.setup_tracer` (`src/tracing.py`)
- Tests: `tests/test_github_producer.py` (`tests/test_github_producer.py`)
- Compose: `docker-compose.yml` (`docker-compose.yml`)
- Requirements: `requirements.txt` (`requirements.txt`)

Contato
-------
Abra issues e PRs no repositório. Siga as guidelines acima para contribuições.
# GitHub Events Real-Time Streaming Analytics

## Architecture
- **Data Source**: GitHub Events API (streaming)
- **Message Broker**: Apache Kafka
- **Stream Processing**: Apache Spark Streaming
- **Storage**: AWS S3 / Local Parquet
- **Visualization**: Dashboard com métricas em tempo real

## Metrics to Track
- Most active repositories (commits/hour)
- Programming language trends
- Geographic distribution of developers
- Event type distribution (push, PR, issues, stars)
- Sentiment analysis on commit messages

## Tech Stack
- Python 3.11+
- Apache Kafka
- Apache Spark (PySpark)
- Docker & Docker Compose
- Pandas, Plotly for viz
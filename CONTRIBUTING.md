# Contributing

Thanks for wanting to contribute! This project aims to be welcoming and professional for contributors in Brazil and North America.

How to contribute
- Fork the repository and create a branch named `feature/description` or `fix/description`.
- Keep changes small and focused. Open a single PR per concern.
- Write tests for new behavior and ensure existing tests pass: `pytest -q`.
- Run formatting and linters before opening a PR:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
black .
flake8
mypy src --ignore-missing-imports
pytest -q
```

Style & code review
- Follow existing code style (Black formatting).
- Add type hints where practical and keep runtime overhead low.
- Document public functions and modules.

Issues and feature requests
- Open issues for bugs or proposals. Use the provided templates.

Communicating
- Use English or Portuguese in issues and PRs. Be descriptive and include logs or error output.

Thank you for improving this project!

Runbook highlights
- Start infra: `docker compose up -d`
- Quick smoke test: `python scripts/smoke_test.py`
- Avro demo: `python scripts/register_schema_and_produce.py --count 5` then `python scripts/consume_avro_demo.py` to decode

If you'd like, open an issue and request that I prepare a PR with screenshots/GIFs for the README.

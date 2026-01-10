# Repository Guidelines

## Project Structure and Module Organization
- `src/blossom/` is the core library. Major areas: `context/`, `dataset/`, `dataframe/`, `op/`, `provider/`, `schema/`, `util/`.
- `example/` contains runnable scripts and sample data at `example/data/`.
- `config.yaml.example` provides a template for local model provider settings; do not commit secrets in `config.yaml`.
- `dist/` holds built artifacts when packaging locally.
- `README.md` and `README_EN.md` are the primary docs.

## Build, Test, and Development Commands
- `poetry install` sets up the dev environment.
- `poetry run python example/chat_distill.py` runs a local example pipeline.
- `poetry run pytest` runs the unit test suite.
- `poetry build` creates sdist and wheel packages.
- `poetry run black src example tests` formats code.
- `poetry run ruff check src example tests` runs lint rules from `pyproject.toml`.
- `poetry run mypy src` runs type checks (optional, aligns with `py.typed`).

## Coding Style and Naming Conventions
Use 4-space indentation and keep code Black-compatible. Favor type hints for public APIs. Name modules and functions in `snake_case`, classes in `PascalCase`, and constants in `UPPER_CASE`. Keep operator classes and functions in `src/blossom/op/` and add public exports in `src/blossom/op/__init__.py` when needed.

## Testing Guidelines
Unit tests live under `tests/` and are executed with pytest (`poetry run pytest`). Use `test_*.py` naming for new tests. If a test depends on Ray or Spark, mark it with `@pytest.mark.ray` or `@pytest.mark.spark` (markers are defined in `pyproject.toml`).

## Commit and Pull Request Guidelines
Commit history follows a Conventional Commit-style prefix such as `feat:`, `fix:`, `refactor:`, or `chore:` with a short, imperative summary. PRs should include a brief description, links to issues when applicable, and notes on config or API changes. If you change examples or operators, include the exact command you used to validate behavior.

## Configuration and Secrets
Configuration is discovered in order: `BLOSSOM_CONFIG`, `./config.yaml`, then `~/.blossom.yaml`. Keep API keys out of git and rely on `config.yaml.example` for shared defaults.

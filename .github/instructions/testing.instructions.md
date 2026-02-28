---
applyTo: "azure-function/tests/**"
---

# Testing Instructions

## Overview

This project uses **pytest**. Tests live in `azure-function/tests/`. Do NOT use `npx`, `jest`, `vitest`, or any JavaScript test runners — this is a Python project.

## Running Tests

All commands must be run from the `azure-function/` directory.

```bash
# Run all tests
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_<module>.py -v

# Run a specific test class or test
python -m pytest tests/test_<module>.py::TestClassName -v
python -m pytest tests/test_<module>.py::TestClassName::test_name -v

# Run with coverage
python -m pytest tests/ --cov=services --cov-report=term-missing -v
```

## Where to Find Things

* **Pytest and coverage configuration:** Read [azure-function/pyproject.toml](../../azure-function/pyproject.toml)
* **Shared fixtures, helpers, and constants:** Read [azure-function/tests/conftest.py](../../azure-function/tests/conftest.py) — always read this file before writing or modifying tests
* **JSON fixture data (mock API responses):** Browse [azure-function/tests/fixtures/](../../azure-function/tests/fixtures/)
* **Existing tests:** Browse the test files in [azure-function/tests/](../../azure-function/tests/) — always read existing tests for the module you're modifying to understand the established patterns before writing new ones

## Conventions

These conventions are used consistently across all test files. Follow them when writing new tests.

### File & Naming Patterns

* One test file per module: `test_<module_name>.py`
* Test classes group related tests: `class TestClassName:` (PascalCase, describes what's being tested)
* Test functions describe behavior: `test_<behavior_being_tested>` (snake_case, reads like a sentence)

### Fixtures & Mocking

* **Environment variables are automatically mocked** via an `autouse=True` fixture in [conftest.py](../../azure-function/tests/conftest.py). Tests never need real API keys. Read [conftest.py](../../azure-function/tests/conftest.py) to see which variables are mocked.
* **JSON fixtures** in [tests/fixtures/](../../azure-function/tests/fixtures/) provide mock API responses. They are loaded via pytest fixtures defined in [conftest.py](../../azure-function/tests/conftest.py). Check what's available there before creating new fixture files.
* **Shared test constants** (test place names, IDs, paths) are defined at the top of [conftest.py](../../azure-function/tests/conftest.py).
* **Use `unittest.mock`** — the project uses `mock.patch` and `mock.MagicMock`. Read existing tests in the same file to see the mocking patterns for that module's dependencies.

### Principles

* **Read before writing** — always read the existing tests for the module and [conftest.py](../../azure-function/tests/conftest.py) before adding new tests
* **Mock all external calls** — no test should make real API calls to Airtable, Google Maps, Outscraper, Cosmos DB, or OpenAI
* **Test both success and failure paths** — include exception handling and edge case tests
* **Keep tests independent** — no test should depend on another test's state or execution order
* **Match the existing style** — follow the mocking approach, class grouping, and naming patterns you see in the test file you're editing
* **Add JSON fixtures for new API responses** — place them in [tests/fixtures/](../../azure-function/tests/fixtures/) and load them via a fixture in [conftest.py](../../azure-function/tests/conftest.py)

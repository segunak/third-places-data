# Copilot Instructions

## Project Overview

* **Name:** Third Places Data
* **Tech Stack:** Python, Azure Functions (Durable Functions), Airtable, Google Maps API, Outscraper API, Azure Cosmos DB, Azure Data Lake
* **Purpose:** A collection of data operations for aggregating information about local third places (cafes, coffee shops, libraries, etc.) into a single platform. Handles data fetching, enrichment, and transformation for city-based third place directories.
* **Architecture:** Azure Functions app using the v2 Python programming model with Durable Functions for orchestration. Blueprint-based function registration with service classes for business logic.

## Where to Find Things

* **App entry point:** Read [azure-function/function_app.py](../azure-function/function_app.py) to see how blueprints are registered and the app is wired together
* **Function definitions:** Browse [azure-function/blueprints/](../azure-function/blueprints/) — each file is a blueprint grouping related HTTP triggers, orchestrators, and activity functions
* **Business logic:** Browse [azure-function/services/](../azure-function/services/) — each file is a service class encapsulating logic for a specific domain (Airtable, Google Maps, Cosmos DB, etc.)
* **Shared constants and enums:** Read [azure-function/constants.py](../azure-function/constants.py)
* **Test suite:** See [azure-function/tests/](../azure-function/tests/) and its [conftest.py](../azure-function/tests/conftest.py) for fixtures and shared setup
* **Dependencies:** See [azure-function/requirements.txt](../azure-function/requirements.txt)
* **Test and coverage config:** See [azure-function/pyproject.toml](../azure-function/pyproject.toml)
* **CI/CD workflows:** See [.github/workflows/](./workflows/)
* **Project documentation:** See [docs/](../docs/)

## Key Patterns

Before writing new code, read the existing files in the relevant directory to understand established patterns. The codebase follows these consistently:

* **Blueprint registration:** Function definitions live in [blueprints/](../azure-function/blueprints/) and are registered in [function_app.py](../azure-function/function_app.py). New functions go in the appropriate blueprint file.
* **Service classes:** Business logic lives in [services/](../azure-function/services/). Blueprints call service methods — they don't contain business logic themselves.
* **Provider pattern:** `PlaceDataProviderFactory` in [services/place_data_service.py](../azure-function/services/place_data_service.py) creates provider instances based on a `provider_type` parameter. Read it before adding new data providers.
* **Durable orchestration:** Long-running operations use Durable Functions with fan-out/fan-in. Read an existing orchestrator in [blueprints/](../azure-function/blueprints/) to understand the pattern before writing a new one.
* **Single fetch, fan-out:** Airtable records are fetched once via the `get_all_third_places` activity, then individual records are passed to per-place activities. Never call `all_third_places` inside per-place activities — this avoids Airtable rate limits.

## Development Guidelines

### Code Style

* Use Python type hints for function signatures
* Follow the patterns you see in existing code — read neighboring functions before writing new ones
* Use the `logging` module for all log output (not `print`)
* Environment variables are loaded via `python-dotenv` and accessed with `os.environ`
* Keep service logic in [services/](../azure-function/services/) and HTTP/orchestration wiring in [blueprints/](../azure-function/blueprints/)

### Naming Conventions

* Functions: `snake_case`
* Classes: `PascalCase`
* Files: `snake_case`
* Test files: `test_<module_name>.py`
* Constants/Enum values: `UPPER_SNAKE_CASE`; Enum classes: `PascalCase`

### Working Directory

* Always run commands from the `azure-function/` subdirectory for pytest, pip, and func commands
* Virtual environment is at `azure-function/.venv/`

### Setup & Running

* `pip install -r requirements.txt` — install dependencies (from `azure-function/`)
* `func host start` — run Azure Functions locally (from `azure-function/`)

### Environment Variables

Required environment variables are listed in `azure-function/.env` (or [local.settings.json](../azure-function/local.settings.json)). In tests, they are automatically mocked — see [tests/conftest.py](../azure-function/tests/conftest.py) for the full list.

## Communication Style

* Be direct and factual in responses
* Avoid apologetic language or unnecessary agreement
* Focus on practical solutions over enthusiasm
* Question incorrect assumptions with facts
* Avoid hyperbole; maintain professional tone
* Write comments for long-term code clarity, not temporary changes

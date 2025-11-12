# Tests for snakemake-storage-plugin-pelican

This directory contains unit tests for the Pelican storage plugin. All tests use mocking to avoid requiring a real Pelican federation.

## Test Structure

- `test_plugin.py` - Main test suite with multiple test classes:
  - `TestStorage` - Inherits from Snakemake's `TestStorageBase` to test basic storage operations
  - `TestPelicanURLNormalization` - Tests OSDF URL normalization (2 vs 3 slashes)
  - `TestPelicanURLConversion` - Tests OSDF-to-Pelican URL conversion
  - `TestTokenMapping` - Tests token file mapping and selection logic
  - `TestQueryValidation` - Tests query URL validation
  - `TestStorageObjectBasics` - Tests basic StorageObject functionality

- `conftest.py` - Shared pytest fixtures for creating temporary token files

## Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run coverage run -m pytest
poetry run coverage report -m

# Run specific test class
poetry run pytest tests/test_plugin.py::TestTokenMapping

# Run specific test
poetry run pytest tests/test_plugin.py::TestTokenMapping::test_longest_prefix_match

# Run with verbose output
poetry run pytest -v

# Run only fast tests (exclude slow/integration)
poetry run pytest -m "not slow and not integration"
```

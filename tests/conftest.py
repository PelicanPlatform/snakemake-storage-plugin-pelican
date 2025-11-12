"""
Pytest configuration and shared fixtures for snakemake-storage-plugin-pelican tests.
"""

import pytest


@pytest.fixture
def tmp_token_file(tmp_path):
    """Create a temporary token file for testing."""
    token_file = tmp_path / "test-token.txt"
    token_file.write_text("test-token-content-12345")
    return token_file


@pytest.fixture
def multiple_token_files(tmp_path):
    """Create multiple token files for testing token mapping."""
    tokens = {}

    for i, name in enumerate(["osdf", "chtc", "ospool", "default"], start=1):
        token_file = tmp_path / f"{name}-token.txt"
        token_file.write_text(f"{name}-token-content-{i}")
        tokens[name] = str(token_file)

    return tokens

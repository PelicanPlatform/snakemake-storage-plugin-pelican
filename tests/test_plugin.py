import os
import pytest
from typing import Optional, Type
from unittest.mock import Mock, MagicMock
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_storage_plugin_pelican import StorageProvider, StorageProviderSettings


class TestStorage(TestStorageBase):
    __test__ = True
    # set to True if the storage is read-only
    retrieve_only = False
    # set to True if the storage is write-only
    store_only = False
    # set to False if the storage does not support deletion
    delete = False  # Pelican objects are immutable, cannot delete
    # set to True if the storage object implements support for touching (inherits from
    # StorageObjectTouch)
    touch = False
    # set to False if also directory upload/download should be tested (if your plugin
    # supports directory down-/upload, definitely do that)
    files_only = True

    def get_query(self, tmp_path) -> str:
        # Return a query for a file that exists in our mock federation
        return "pelican://test-federation.org/namespace/test-file.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        # Return a query for a file that doesn't exist in our mock federation
        return "pelican://test-federation.org/namespace/non-existent-file.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # Return settings with None values to avoid AttributeError
        return StorageProviderSettings(token_file=None, debug=None)

    @pytest.fixture(autouse=True)
    def mock_pelicanfs(self, monkeypatch):
        """Mock PelicanFileSystem to avoid needing a real federation."""

        # Create a mock filesystem with necessary methods
        mock_fs = MagicMock()

        # Mock exists() - test-file.txt exists, non-existent-file.txt doesn't
        def mock_exists(path):
            if "test-file.txt" in path:
                return True
            return False

        mock_fs.exists = Mock(side_effect=mock_exists)

        # Mock info() - return size for existing files
        def mock_info(path):
            if "test-file.txt" in path:
                return {"size": 1234, "type": "file"}
            raise FileNotFoundError(f"File not found: {path}")

        mock_fs.info = Mock(side_effect=mock_info)

        # Mock get() - simulate file retrieval
        def mock_get(remote_path, local_path):
            # Create the local file with some content
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w") as f:
                f.write("test content from pelican")

        mock_fs.get = Mock(side_effect=mock_get)

        # Mock put_file() - simulate file upload
        def mock_put_file(local_path, remote_path):
            # Just verify the local file exists
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")

        mock_fs.put_file = Mock(side_effect=mock_put_file)

        # Create a mock PelicanFileSystem class that returns our mock instance
        def mock_pelican_fs_init(*args, **kwargs):
            return mock_fs

        # Patch the PelicanFileSystem constructor
        monkeypatch.setattr(
            "snakemake_storage_plugin_pelican.PelicanFileSystem", mock_pelican_fs_init
        )


# Additional unit tests for Pelican-specific functionality
class TestPelicanURLNormalization:
    """Test OSDF URL normalization."""

    def test_normalize_osdf_slashes_two_slashes(self):
        from snakemake_storage_plugin_pelican import _normalize_osdf_slashes

        url = "osdf://hostname/path/to/file"
        normalized = _normalize_osdf_slashes(url)
        assert normalized == "osdf:///hostname/path/to/file"

    def test_normalize_osdf_slashes_three_slashes(self):
        from snakemake_storage_plugin_pelican import _normalize_osdf_slashes

        url = "osdf:///path/to/file"
        normalized = _normalize_osdf_slashes(url)
        assert normalized == "osdf:///path/to/file"

    def test_normalize_osdf_slashes_non_osdf(self):
        from snakemake_storage_plugin_pelican import _normalize_osdf_slashes

        url = "pelican://host/path"
        normalized = _normalize_osdf_slashes(url)
        assert normalized == url


class TestPelicanURLConversion:
    """Test OSDF to Pelican URL conversion."""

    def test_osdf_to_pelican_conversion(self):
        from snakemake_storage_plugin_pelican import _get_pelican_url_if_needed

        osdf_url = "osdf:///namespace/path/to/file.txt"
        pelican_url = _get_pelican_url_if_needed(osdf_url)
        assert pelican_url == "pelican://osg-htc.org/namespace/path/to/file.txt"

    def test_pelican_url_unchanged(self):
        from snakemake_storage_plugin_pelican import _get_pelican_url_if_needed

        pelican_url = "pelican://custom-federation.org/path/to/file.txt"
        result = _get_pelican_url_if_needed(pelican_url)
        assert result == pelican_url


class TestTokenMapping:
    """Test token mapping and selection logic."""

    @pytest.fixture
    def mock_token_files(self, tmp_path):
        """Create mock token files for testing."""
        token1 = tmp_path / "token1.txt"
        token1.write_text("token-content-1")

        token2 = tmp_path / "token2.txt"
        token2.write_text("token-content-2")

        default_token = tmp_path / "default.txt"
        default_token.write_text("default-token-content")

        return {
            "token1": str(token1),
            "token2": str(token2),
            "default": str(default_token),
        }

    def test_single_token_mapping(self, tmp_path, mock_token_files, monkeypatch):
        """Test simple single token configuration."""
        from snakemake_storage_plugin_pelican import (
            StorageProvider,
            StorageProviderSettings,
        )
        import logging

        # Mock PelicanFileSystem
        mock_fs = MagicMock()
        monkeypatch.setattr(
            "snakemake_storage_plugin_pelican.PelicanFileSystem",
            lambda *args, **kwargs: mock_fs,
        )

        settings = StorageProviderSettings(token_file=mock_token_files["default"])
        provider = StorageProvider(
            logger=logging.getLogger(__name__),
            local_prefix=tmp_path / "local",
            settings=settings,
        )

        # Should have one default token mapping
        assert "" in provider._token_mappings
        assert provider._token_mappings[""] == mock_token_files["default"]

    def test_multiple_token_mappings(self, tmp_path, mock_token_files, monkeypatch):
        """Test multiple token mappings with URL prefixes."""
        from snakemake_storage_plugin_pelican import (
            StorageProvider,
            StorageProviderSettings,
        )
        import logging

        # Mock PelicanFileSystem
        mock_fs = MagicMock()
        monkeypatch.setattr(
            "snakemake_storage_plugin_pelican.PelicanFileSystem",
            lambda *args, **kwargs: mock_fs,
        )

        # Configure multiple token mappings
        token_config = (
            f"pelican://osg-htc.org/chtc:{mock_token_files['token1']} "
            f"pelican://osg-htc.org/ospool:{mock_token_files['token2']}"
        )

        settings = StorageProviderSettings(token_file=token_config)
        provider = StorageProvider(
            logger=logging.getLogger(__name__),
            local_prefix=tmp_path / "local",
            settings=settings,
        )

        # Should have two URL prefix mappings
        assert "pelican://osg-htc.org/chtc" in provider._token_mappings
        assert "pelican://osg-htc.org/ospool" in provider._token_mappings
        assert (
            provider._token_mappings["pelican://osg-htc.org/chtc"]
            == mock_token_files["token1"]
        )
        assert (
            provider._token_mappings["pelican://osg-htc.org/ospool"]
            == mock_token_files["token2"]
        )

    def test_longest_prefix_match(self, tmp_path, mock_token_files, monkeypatch):
        """Test that longest matching URL prefix wins."""
        from snakemake_storage_plugin_pelican import (
            StorageProvider,
            StorageProviderSettings,
        )
        import logging

        # Mock PelicanFileSystem
        mock_fs = MagicMock()
        monkeypatch.setattr(
            "snakemake_storage_plugin_pelican.PelicanFileSystem",
            lambda *args, **kwargs: mock_fs,
        )

        # Configure overlapping prefixes
        token_config = (
            f"pelican://osg-htc.org:{mock_token_files['default']} "
            f"pelican://osg-htc.org/chtc:{mock_token_files['token1']}"
        )

        settings = StorageProviderSettings(token_file=token_config)
        provider = StorageProvider(
            logger=logging.getLogger(__name__),
            local_prefix=tmp_path / "local",
            settings=settings,
        )

        # Query under /chtc should match the longer prefix
        query = "pelican://osg-htc.org/chtc/user/file.txt"
        token = provider._get_token_for_query(query)
        assert token == "token-content-1"  # from token1.txt

        # Query under different namespace should match shorter prefix
        query2 = "pelican://osg-htc.org/ospool/user/file.txt"
        token2 = provider._get_token_for_query(query2)
        assert token2 == "default-token-content"  # from default.txt


class TestQueryValidation:
    """Test query validation logic."""

    def test_valid_pelican_url(self):
        from snakemake_storage_plugin_pelican import StorageProvider

        result = StorageProvider.is_valid_query(
            "pelican://federation.org/namespace/file.txt"
        )
        assert result.valid is True

    def test_valid_osdf_url(self):
        from snakemake_storage_plugin_pelican import StorageProvider

        result = StorageProvider.is_valid_query("osdf:///namespace/file.txt")
        assert result.valid is True

    def test_invalid_scheme(self):
        from snakemake_storage_plugin_pelican import StorageProvider

        result = StorageProvider.is_valid_query("http://example.com/file.txt")
        assert result.valid is False
        assert "pelican://" in result.reason or "osdf://" in result.reason

    def test_pelican_url_without_hostname(self):
        from snakemake_storage_plugin_pelican import StorageProvider

        result = StorageProvider.is_valid_query("pelican:///path/to/file")
        assert result.valid is False
        assert "hostname" in result.reason.lower()


class TestStorageObjectBasics:
    """Test basic StorageObject functionality with mocked PelicanFS."""

    @pytest.fixture
    def mock_provider(self, tmp_path, monkeypatch):
        """Create a mock provider with mocked PelicanFileSystem."""
        from snakemake_storage_plugin_pelican import (
            StorageProvider,
            StorageProviderSettings,
        )
        import logging

        # Mock PelicanFileSystem
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        mock_fs.info.return_value = {"size": 1234, "type": "file"}

        monkeypatch.setattr(
            "snakemake_storage_plugin_pelican.PelicanFileSystem",
            lambda *args, **kwargs: mock_fs,
        )

        settings = StorageProviderSettings()
        provider = StorageProvider(
            logger=logging.getLogger(__name__),
            local_prefix=tmp_path / "local",
            settings=settings,
        )

        return provider, mock_fs

    def test_local_suffix_extraction(self, mock_provider):
        """Test that local_suffix correctly extracts filename."""
        from snakemake_storage_plugin_pelican import StorageObject

        provider, _ = mock_provider

        # Create storage object
        query = "pelican://test-federation.org/namespace/path/to/myfile.txt"
        obj = StorageObject(
            query=query, keep_local=False, retrieve=False, provider=provider
        )

        suffix = obj.local_suffix()
        assert suffix == "myfile.txt"

    def test_path_extraction(self, mock_provider):
        """Test that path is correctly extracted from query."""
        from snakemake_storage_plugin_pelican import StorageObject

        provider, _ = mock_provider

        query = "pelican://test-federation.org/namespace/path/to/file.txt"
        obj = StorageObject(
            query=query, keep_local=False, retrieve=False, provider=provider
        )

        assert obj._path == "/namespace/path/to/file.txt"

    def test_exists_check(self, mock_provider):
        """Test existence checking."""
        from snakemake_storage_plugin_pelican import StorageObject

        provider, mock_fs = mock_provider

        query = "pelican://test-federation.org/namespace/existing-file.txt"
        obj = StorageObject(
            query=query, keep_local=False, retrieve=False, provider=provider
        )

        # Mock returns True
        assert obj.exists() is True
        mock_fs.exists.assert_called_with("/namespace/existing-file.txt")

    def test_size_retrieval(self, mock_provider):
        """Test size retrieval."""
        from snakemake_storage_plugin_pelican import StorageObject

        provider, mock_fs = mock_provider

        query = "pelican://test-federation.org/namespace/file.txt"
        obj = StorageObject(
            query=query, keep_local=False, retrieve=False, provider=provider
        )

        size = obj.size()
        assert size == 1234
        mock_fs.info.assert_called_with("/namespace/file.txt")

    def test_mtime_returns_zero(self, mock_provider):
        """Test that mtime always returns 0 (Pelican objects are immutable)."""
        from snakemake_storage_plugin_pelican import StorageObject

        provider, _ = mock_provider

        query = "pelican://test-federation.org/namespace/file.txt"
        obj = StorageObject(
            query=query, keep_local=False, retrieve=False, provider=provider
        )

        # Pelican objects are immutable, so mtime should always be 0
        assert obj.mtime() == 0.0

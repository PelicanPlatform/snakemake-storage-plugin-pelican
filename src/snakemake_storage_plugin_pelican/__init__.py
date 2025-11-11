import logging
import os
from pathlib import PurePosixPath
from pelicanfs.core import PelicanFileSystem
from urllib.parse import urlparse

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, List
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface

# Raise errors that will not be handled within this plugin but thrown upwards to
# Snakemake and the user as WorkflowError.
from snakemake_interface_common.exceptions import WorkflowError  # noqa: F401


def _normalize_osdf_slashes(url: str) -> str:
    """
    Given a user-supplied OSDF url, normalize its slashes.
    A normalized OSDF url always has the requisite 3 slashes after the scheme.
    """
    parsed = urlparse(url)
    if parsed.scheme != "osdf":
        return url
    if parsed.hostname is not None:  # they used 2 slashes instead of 3
        return f"osdf:///{parsed.hostname}{parsed.path}"
    return url


def _get_pelican_url_if_needed(query: str) -> str:
    """
    Convert OSDF Urls to Pelican URLs.
    If the user supplied an OSDF url, grab the path component and convert
    it to a pelican url by hardcoding the osg-htc.org federation.
    """
    parsed = urlparse(query)
    if parsed.scheme == "osdf":
        parsed_pelican = urlparse(_normalize_osdf_slashes(query))
        # Convert to pelican url
        return f"pelican://osg-htc.org{parsed_pelican.path}"
    return query


def _parse_single_token_string(value: str, mappings: dict):
    """
    Helper to parse a single token string and add to mappings dict.

    The value may contain multiple space-separated components.
    Each component is in the format "URL_prefix:token_path"

    Examples:
      - "pelican://host/path:token.txt"
      - "pelican://host:443/path:token.txt" (with port)
      - "pelican://host1/path:tok1.txt pelican://host2/path:tok2.txt" (multiple)

    Strategy: Split by spaces first, then for each component, the last colon
    separates the URL prefix from the token file path.
    """
    if not value:
        return

    # Split by spaces to handle multiple components in one string
    components = value.split()

    for component in components:
        if not component:
            continue

        # Check for URL-tagged value (pelican:// or osdf://)
        if "pelican://" in component or "osdf://" in component:
            # Find the last colon - this separates the URL from the token path
            # This assumes the format: pelican://host:port/path:token.txt
            # where the LAST colon is always the separator
            last_colon_pos = component.rfind(":")

            if last_colon_pos > 0:
                url_prefix = component[:last_colon_pos]
                token_path = component[last_colon_pos + 1 :]

                # Only add if both parts are non-empty
                if url_prefix and token_path:
                    mappings[url_prefix] = token_path
                    continue

        # Check for simple "tag:value" format
        if ":" in component and not component.startswith("/"):
            parts = component.split(":", 1)
            if len(parts) == 2:
                tag, token_path = parts
                if tag.lower() in ["default", ""]:
                    mappings[""] = token_path
                else:
                    mappings[tag] = token_path
                continue

        # No tag found, treat as untagged default token
        mappings[""] = component


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    token_file: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Path to a file containing a Pelican authorization token. "
                "Can specify multiple space-separated token mappings (in quotes), each tagged with a Pelican URL prefix. "
                "Tags should be pelican:// URLs (can include path prefix). "
                "The longest matching URL prefix wins. "
                "Examples: "
                "(1) Single token for all: --storage-pelican-token-file /path/to/token.txt | "
                "(2) Multiple tokens (space-separated in quotes): "
                "--storage-pelican-token-file 'pelican://osg-htc.org:/path/to/osg.txt pelican://itb-osdf-director.osdf-dev.chtc.io:/path/to/itb.txt' | "
                "(3) Per-namespace: "
                "--storage-pelican-token-file 'pelican://osg-htc.org/chtc:/path/to/chtc.txt pelican://osg-htc.org/ospool:/path/to/ospool.txt' | "
                "(4) With default: "
                "--storage-pelican-token-file 'pelican://osg-htc.org/chtc/itb:/path/to/itb.txt default:/path/to/default.txt'"
            ),
            "env_var": False,
            "required": False,
        },
    )
    debug: Optional[str] = field(
        default=None,
        metadata={
            "help": "Enable debug logging for the Pelican Storage Plugin. Use: --storage-pelican-debug true",
            "env_var": False,
            "required": False,
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
# Inside of the provider, you can use self.logger (a normal Python logger of type
# logging.Logger) to log any additional informations or
# warnings.
#
# Here, StorageProvider is a singleton class that is instantiated once per Snakemake
# invocation -- it handles _all_ Pelican storage interactions for any number of
# federations/storage objects.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    # A cache for loaded tokens (token_file_path -> token_string)
    _token_cache: dict[str, str] = None

    # A cache for holding onto PelicanFileSystem objects
    # Key: (federation, token_hash) -> PelicanFileSystem
    _fs_cache: dict[str, PelicanFileSystem] = None

    # Parsed token mappings (URL prefix -> token file path)
    _token_mappings: dict[str, str] = None

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self._token_cache = {}
        self._fs_cache = {}

        self._set_debugging()
        self._map_tokens()

    def _map_tokens(self):
        # Parse token mappings from the token_file setting
        # self.settings.token_file is a single string that may contain multiple
        # space-separated token mappings (e.g., "pelican://host1:tok1.txt pelican://host2:tok2.txt")
        self._token_mappings = {}

        if self.settings.token_file:
            self.logger.debug(f"Raw token_file settings: {self.settings.token_file}")

            import re

            # Split by space followed by a URL scheme (pelican:// or osdf://), preserving the scheme
            # This allows us to handle multiple token mappings in a single string
            parts = re.split(r"\s+(?=(?:pelican|osdf)://)", self.settings.token_file)

            self.logger.debug(
                f"Split input token list into {len(parts)} token mapping parts"
            )

            for idx, part in enumerate(parts):
                if part.strip():
                    self.logger.debug(f"Parsing token mapping [{idx}]: {part.strip()}")
                    _parse_single_token_string(part.strip(), self._token_mappings)

        # Log loaded token mappings at DEBUG level (shows in verbose mode)
        if self._token_mappings:
            self.logger.debug(f"Loaded {len(self._token_mappings)} token mapping(s)")
            for prefix, token_file in self._token_mappings.items():
                prefix_display = prefix if prefix else "(default/untagged)"
                self.logger.debug(f"  • {prefix_display} -> {token_file}")
        else:
            self.logger.debug(
                "No token mappings configured (anonymous access will be used)"
            )

    def _set_debugging(self):
        # Set log levels for PelicanFS and the storage plugin if `debug` is enabled
        # Accept any truthy value: "true", "True", "1", "yes", etc.
        debug_enabled = self.settings.debug and self.settings.debug.lower() in (
            "true",
            "1",
            "yes",
            "on",
        )

        if debug_enabled:
            # Enable DEBUG logging for PelicanFS (fsspec.pelican)
            pelFSlogger = logging.getLogger("fsspec.pelican")
            pelFSlogger.setLevel(logging.DEBUG)

            # Enable DEBUG logging for this plugin's logger (self.logger)
            self.logger.setLevel(logging.DEBUG)

            # Ensure there's a handler to actually output the logs
            if not self.logger.handlers:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.DEBUG)
                formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
                console_handler.setFormatter(formatter)
                self.logger.addHandler(console_handler)

    def _load_token(self, token_path: str) -> str:
        """Load and cache a token from a file."""
        if token_path not in self._token_cache:
            self.logger.debug(f"Loading token from file: {token_path}")
            try:
                with open(token_path, "r") as f:
                    self._token_cache[token_path] = f.read().strip()
                self.logger.debug(
                    f"Successfully loaded token from {token_path} (length: {len(self._token_cache[token_path])})"
                )
            except Exception as e:
                raise WorkflowError(f"Failed to load token from {token_path}: {e}")
        else:
            self.logger.debug(f"Using cached token from {token_path}")

        return self._token_cache[token_path]

    def _get_token_for_query(self, query: str) -> Optional[str]:
        """
        Get the appropriate token for a query based on pelican:// URL prefix tags.

        Token selection uses longest-matching-URL-prefix algorithm:
        1. Normalize the query to pelican:// format
        2. Check all parsed token mappings for URL prefixes
        3. Return the token whose URL prefix is the longest match
        4. Fall back to untagged/default token if no prefix matches
        """
        self.logger.debug(f"Getting token for query: {query}")

        if not self._token_mappings:
            self.logger.debug("No token mappings available")
            return None

        # Normalize the query to pelican:// format
        normalized_query = _get_pelican_url_if_needed(query)
        self.logger.debug(
            f"Normalized query used to check for token matches: {normalized_query}"
        )

        # Find the longest matching URL prefix from our parsed mappings
        best_match = None
        best_match_length = 0
        best_match_prefix = None

        for url_prefix, token_path in self._token_mappings.items():
            # Empty string means default/untagged
            if not url_prefix:
                continue

            # Normalize the URL prefix (in case it was osdf://)
            normalized_prefix = _get_pelican_url_if_needed(url_prefix)

            # Check if the query starts with this prefix
            if normalized_query.startswith(normalized_prefix):
                # Ensure it's a proper prefix match
                # The prefix already ends with '/' in most cases, so we just need to verify
                # the query continues after the prefix (or is an exact match)
                if normalized_query == normalized_prefix or len(normalized_query) > len(
                    normalized_prefix
                ):
                    # Valid prefix match
                    self.logger.debug(
                        f"  Prefix match: '{normalized_prefix}' (length: {len(normalized_prefix)})"
                    )
                    if len(normalized_prefix) > best_match_length:
                        best_match = token_path
                        best_match_length = len(normalized_prefix)
                        best_match_prefix = normalized_prefix

        if best_match:
            self.logger.debug(
                f"Selected token file '{best_match}' for prefix '{best_match_prefix}'"
            )
            return self._load_token(best_match)

        # Fall back to default/untagged token
        if "" in self._token_mappings:
            self.logger.debug(
                f"Using default/untagged token: {self._token_mappings['']}"
            )
            return self._load_token(self._token_mappings[""])

        # No matching token found
        self.logger.debug("No matching token found for query")
        return None

    def _get_filesystem(self, query: str) -> PelicanFileSystem:
        """
        Get or create a cached PelicanFileSystem for the given query's federation.
        The filesystem is cached by (federation_hostname, token) to ensure we reuse
        filesystem objects for the same federation and authentication.
        """
        self.logger.debug(f"Getting filesystem for query: {query}")

        parsed = urlparse(_get_pelican_url_if_needed(query))
        federation = parsed.hostname

        if not federation:
            self.logger.error(f"Cannot determine federation from query: {query}")
            raise WorkflowError(f"Cannot determine federation from query: {query}")

        self.logger.debug(f"Federation hostname: {federation}")

        token = self._get_token_for_query(query)
        if token:
            self.logger.debug(
                f"Creating filesystem for federation '{federation}' with a token (token length: {len(token)} chars)"
            )
        else:
            self.logger.warning(
                f"No token found for query '{query}' - attempting anonymous access"
            )
            if self._token_mappings:
                self.logger.debug(
                    f"Available token prefix mappings: {list(self._token_mappings.keys())}"
                )

        # Create cache key combining federation and whether we're using a token
        # (we hash the token to avoid storing it in the key)
        cache_key = f"{federation}:{hash(token) if token else 'no-token'}"

        if cache_key not in self._fs_cache:
            # PelicanFileSystem expects a federation discovery URL (https://...)
            # not a pelican:// URL
            discovery_url = f"https://{federation}/"

            if token:
                headers = {"Authorization": f"Bearer {token}"}
                self.logger.debug(
                    f"Creating new PelicanFileSystem for {federation} with authentication"
                )
                self._fs_cache[cache_key] = PelicanFileSystem(
                    discovery_url, headers=headers
                )
            else:
                self.logger.debug(
                    f"Creating new PelicanFileSystem for {federation} (anonymous)"
                )
                self._fs_cache[cache_key] = PelicanFileSystem(discovery_url)
        else:
            self.logger.debug(f"Using cached PelicanFileSystem for {federation}")

        return self._fs_cache[cache_key]

    def _get_path_from_query(self, query: str) -> str:
        """Extract the path component from a pelican/osdf query."""
        self.logger.debug(f"Extracting path from query: {query}")

        parsed = urlparse(_get_pelican_url_if_needed(query))
        path = parsed.path

        if path.endswith("/") and "." in PurePosixPath(path).name:
            path = path.rstrip("/")

        if not path or path == "/":
            self.logger.error(f"Invalid Pelican path '{path}' in query: {query}")
            raise WorkflowError(f"Invalid Pelican path '{path}' in query: {query}")

        self.logger.debug(f"Extracted path: {path}")
        return path

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="pelican://osg-htc.org/namespace/path/object/path",
                type=QueryType.ANY,
                description="An example Pelican URL that points to an object in the osg-htc.org (OSDF) federation.",
            ),
            ExampleQuery(
                query="osdf:///pelicanplatform/test/hello-world.txt",
                type=QueryType.ANY,
                description="The canonical test object in the osg-htc.org (OSDF) federation.",
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        ...

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        ...

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        try:
            parsed = urlparse(query)
        except Exception as e:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason=f"cannot be parsed as URL ({e})",
            )

        if parsed.scheme != "pelican" and parsed.scheme != "osdf":
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="must start with pelican:// or osdf://",
            )
        if parsed.scheme == "pelican" and parsed.hostname is None:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="pelican:// URLs must contain a federation hostname",
            )

        return StorageQueryValidationResult(
            query=query,
            valid=True,
        )

    # If required, overwrite the method postprocess_query from StorageProviderBase
    # in order to e.g. normalize the query or add information from the settings to it.
    # Otherwise, remove this method as it will be inherited from the base class.
    def postprocess_query(self, query: str) -> str:
        return query

    # This can be used to change how the rendered query is displayed in the logs to
    # prevent accidentally printing sensitive information e.g. tokens in a URL.
    def safe_print(self, query: str) -> str:
        """Process the query to remove potentially sensitive information when printing."""
        return query


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
# Inside of the object, you can use self.provider to access the provider (e.g. for )
# self.provider.logger, see above, or self.provider.settings).
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        # pass
        self.provider.logger.debug(
            f"Initializing StorageObject for query: {self.query}"
        )

        self._token = self.provider._get_token_for_query(self.query)
        self._path = self.provider._get_path_from_query(self.query)
        self._fs = self.provider._get_filesystem(self.query)

        self.provider.logger.debug(
            f"StorageObject initialized - path: {self._path}, using token: {self._token is not None}"
        )

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        parsed = urlparse(self.query)
        # Clean the path and get the file at the end of the path
        path = PurePosixPath(parsed.path)
        filename = path.name
        return filename

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        ...

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        self.provider.logger.debug(f"Checking existence of path: {self._path}")

        exists = self._fs.exists(self._path)

        self.provider.logger.debug(f"Path {self._path} exists: {exists}")
        return exists

    @retry_decorator
    def mtime(self) -> float:
        # Snakemake's job is to take input 'a' and derive output 'b' according to some rule.
        # In the case that 'b' already exists for a given 'a', Snakemake must decide whether to
        # re-generate ''b. For an object 'a-prime', this decision is made by checking whether
        # 'a-prime' is newer than 'b' (which would imply a different a was used to derive 'b',
        # and thus 'b' itself must be re-generated). But because Pelican objects are immutable,
        # it must be true that a=a-prime, and thus we're not worried about mtimes.
        # To short-circuit this logic, we aways return the epoch start.
        return 0.0

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        self.provider.logger.debug(f"Getting size of path: {self._path}")

        info = self._fs.info(self._path)

        if "size" not in info:
            self.provider.logger.error(
                f"Cannot determine size of Pelican object '{self.query}'"
            )
            raise WorkflowError(
                f"Cannot determine size of Pelican object '{self.query}'"
            )

        size = info["size"]
        self.provider.logger.debug(f"Size of {self._path}: {size} bytes")
        return size

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        # Optionally, this can make use of the attribute self.is_ondemand_eligible,
        # which indicates that the object could be retrieved on demand,
        # e.g. by only symlinking or mounting it from whatever network storage this
        # plugin provides. For example, objects with self.is_ondemand_eligible == True
        # could mount the object via fuse instead of downloading it.
        # The job can then transparently access only the parts that matter to it
        # without having to wait for the full download.
        # On demand eligibility is calculated via Snakemake's access pattern annotation.
        # If no access pattern is annotated by the workflow developers,
        # self.is_ondemand_eligible is by default set to False.
        local_path = self.local_path()

        self.provider.logger.debug(
            f"Retrieving object from {self._path} to {local_path}"
        )

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self._fs.get(self._path, local_path)
        self.provider.logger.debug(f"Successfully retrieved object to {local_path}")

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        local_path = self.local_path()
        self.provider.logger.debug(f"Storing object from {local_path} to {self._path}")

        # Use put_file() instead of put() to avoid fsspec's directory detection logic.
        # put() checks if the remote path is a directory and appends the filename,
        # but this has been buggy in the past.
        self._fs.put_file(local_path, self._path)

        self.provider.logger.debug(f"Successfully stored object to {self._path}")

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        ...

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        ...

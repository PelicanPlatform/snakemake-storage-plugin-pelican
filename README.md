# Snakemake Pelican Storage Plugin

A [Snakemake storage plugin](https://snakemake.github.io/snakemake-plugin-catalog/) for accessing data via the [Pelican Platform](https://pelicanplatform.org/), enabling integration with data federations like the [Open Science Data Federation (OSDF)](https://osg-htc.org/services/osdf.html).

## Installation

```bash
pip install snakemake-storage-plugin-pelican
```

Or install from source:

```bash
git clone https://github.com/PelicanPlatform/snakemake-storage-plugin-pelican.git
cd snakemake-storage-plugin-pelican
pip install -e .
```

## Usage
There are a few ways to use/configure the plugin.
Broadly, they are:
- Wrapping `pelican://` and `osdf://` URLs with the `storage()` function
- Defining a default storage provider
- Multiple, tagged Pelican plugin instances 

### Using the `storage()` function

In your Snakefile, any files that should be fetched or written to a Pelican federation can be wrapped with the `storage()` function.
This tells Snakemake it should automatically determine which storage plugins can be used to complete the required file operations.
In this case, the Pelican storage plugin will be used whenever the storage string begins with a `pelican://` or `osdf://` URL.

For example:
```python
rule download_data:
    input:
        storage("pelican://osg-htc.org/pelicanplatform/test/hello-world.txt")
    output:
		# Anything not wrapped with `storage()` is assumed to be a local file
        "local_output.txt"
    shell:
        "cp {input} {output}"
```

### Defining a Default Storage Provider

If your workflow heavily relies on objects accessed with Pelican, you may not want to wrap everything explicitly with `storage()`.
In this case, you can define the Pelican plugin as the default storage provider.
This tells Snakemake to assume all files that aren't explicitly wrapped with a different storage provider are Pelican objects.
Enable this feature by passing `--default-storage-provider pelican` to your Snakemake invocation.

Passing this argument also requires defining a default storage prefix to tell Snakemake what common prefix it should prepend to all relative file paths.
This is done with the `--default-storage-prefix` flag.

If all of your files/objects come from the same federation, this prefix will often be the federation URL with a namespace, e.g. `pelican://osg-htc.org/chtc/`.

For example, with this Snakefile:

```python
rule download_data:
    input:
        remote_input="/staging/jhiemstra/input.txt",  # Treated as Pelican (default provider)
        local_input=local("some-local-file.txt")   # Explicitly local with local() wrapper
    output:
        "/staging/jhiemstra/output.txt"  # Also treated as Pelican (default provider)
    shell:
        "cat {input.remote_input} {input.local_input} > {output}"
```

Run with default storage provider:

```bash
snakemake --cores 1 \
    --default-storage-provider pelican \
    --default-storage-prefix "pelican://osg-htc.org/ospool/" \
    --storage-pelican-token-file /path/to/token.txt
```

Snakemake will automatically treat `"/staging/jhiemstra/input.txt"` as `"pelican://osg-htc.org/chtc/staging/jhiemstra/input.txt"`.

### Tagged Plugin Instances

Advanced users can define multiple instances of the Pelican plugin with different configurations using tags.
This is useful when working with multiple federations that require completely separate authentication or settings.

See the [Snakemake storage plugin documentation](https://snakemake.readthedocs.io/en/stable/snakefiles/storage.html) for details on tagged plugin instances.

### URL Formats

The plugin supports two URL schemes:

1. **Pelican URLs**: `pelican://federation-hostname/namespace/path/to/object`
   ```python
   pelican://osg-htc.org/ospool/uchicago/public/data.txt
   ```

2. **OSDF URLs**: `osdf:///namespace/path/to/file` (automatically converted to `pelican://osg-htc.org/...`)
   ```python
   osdf:///pelicanplatform/test/hello-world.txt
   ```

### Authentication with Tokens

The Pelican storage plugin for Snakemake does not yet support automatic token management like other Pelican clients, but these are features that will be added in the future.
In the meantime, whenever an operation on an object requires authorization, you must provide your own tokens.

For information about how to create namespace tokens, see the upstream [Pelican documentation](https://docs.pelicanplatform.org).

#### Single Token for All Requests
If all the Pelican objects in your Snakemake workflow that require authorization can be handled with a single token, that token can be provided to the plugin using the `--storage-pelican-token-file` flag, e.g.: 
```bash
snakemake --cores 1 \
    --storage-pelican-token-file /path/to/token.txt
```

#### Multiple Tokens with URL Prefix Mapping

For workflows accessing multiple federations or namespaces with different credentials, tokens can be mapped to specific federation prefixes.
This is done by passing the `--storage-pelican-token-file` flag a space-delimited string list of `<URL prefix>:<token file>` components.

For example, if your objects come from two different federations, you can specify which tokens should be used with which federations/prefixes:
```bash
snakemake --cores 1 \
    --storage-pelican-token-file "pelican://osg-htc.org/ospool:/path/to/ospool-token.txt pelican://itb-osdf-director.osdf-dev.chtc.io/chtc/itb:/path/to/itb-token.txt"
```
> **Note:** There is a quotation requirement around the string list you pass with this flag.
> The entire list should be wrapped in either single or double quotes.

The plugin uses **longest-prefix matching** to select the appropriate token for each URL.
You can use this feature to map tokens to specific federations or to specific namespaces within a federation.

#### With Default Fallback

```bash
snakemake --cores 1 \
    --storage-pelican-token-file 'pelican://osg-htc.org/chtc/itb:/path/to/itb-token.txt default:/path/to/default-token.txt'
```

URLs that don't match any prefix will use the default token.

### Debug Logging

Enable detailed logging of the Pelican storage plugin and the underlying PelicanFS library for troubleshooting:

```bash
snakemake --cores 1 \
    --storage-pelican-debug true
```

## Examples

### Example 1: Reading from OSDF

```python
rule process_osdf_data:
    input:
        storage("osdf:///pelicanplatform/test/hello-world.txt")
    output:
        "processed_output.txt"
    shell:
        "cat {input} | tr '[:lower:]' '[:upper:]' > {output}"
```

Run without authentication (for public data):

```bash
snakemake --cores 1
```

### Example 2: Reading from Multiple Federations
> **Note:** This example is for demonstration purposes -- the referenced objects do not actually exist, so this Snakefile will not work if copy-pasted.
```python
rule combine_data:
    input:
        osdf_data=storage("pelican://osg-htc.org/ospool/data.txt"),
        itb_data=storage("pelican://itb-osdf-director.osdf-dev.chtc.io/chtc/itb/data.txt")
    output:
        "combined.txt"
    shell:
        "cat {input.osdf_data} {input.itb_data} > {output}"
```

Run with multiple tokens:

```bash
snakemake --cores 1 \
    --storage-pelican-token-file 'pelican://osg-htc.org/ospool:/path/to/ospool-token.txt pelican://itb-osdf-director.osdf-dev.chtc.io/chtc/itb:/path/to/itb-token.txt'
```

### Example 3: Writing to Pelican
In this "write" example, the output is written back to the storage exposed by an Origin.
Here, Snakemake creates an intermediary copy of `output` on the local disk and then uses PelicanFS to copy the data to the remote storage.
```python
rule upload_results:
    input:
        "local_results.txt"
    output:
        storage("pelican://my-federation.org/myspace/results.txt")
    shell:
        "cp {input} {output}"
```

Run with write-enabled token:

```bash
snakemake --cores 1 \
    --storage-pelican-token-file /path/to/write-token.txt
```

## Token File Format

Token files should contain a single line with the bearer token:

```
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Configuration Options

| Option | Description | Example |
|--------|-------------|---------|
| `--storage-pelican-token-file` | Path(s) to token file(s), optionally with URL prefix mappings | `--storage-pelican-token-file 'pelican://host/path:token.txt'` |
| `--storage-pelican-debug` | Enable debug logging | `--storage-pelican-debug true` |

## Dependencies

- [snakemake-interface-storage-plugins](https://github.com/snakemake/snakemake-interface-storage-plugins)
- [pelicanfs](https://github.com/PelicanPlatform/pelicanfs) - Python filesystem interface for Pelican

## Development

To contribute or modify the plugin:

```bash
git clone https://github.com/PelicanPlatform/snakemake-storage-plugin-pelican.git
cd snakemake-storage-plugin-pelican
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

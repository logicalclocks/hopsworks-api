# Dependencies

All Python dependency declarations live in `python/pyproject.toml`.

## Core (always installed)

- `requests` — HTTP client for all REST calls
- `furl` — URL construction and manipulation
- `boto3` — AWS SDK (S3, STS credential chaining)
- `pandas` — DataFrame operations, offline feature data
- `numpy` — numerical utilities
- `pyhumps 1.6.1` — camelCase ↔ snake_case conversion for JSON serialization
- `avro 1.12.0` — Avro serialization for Kafka
- `grpcio`, `protobuf` — gRPC for Arrow Flight and the online store
- `opensearch-py` — OpenSearch integration
- `pyjks` — Java keystore parsing
- `hopsworks-apigen` — `@public`, `@deprecated`, `@also_available_as` decorators
- `fsspec` — filesystem abstraction
- `retrying` — retry decorator for flaky operations

## Optional Extras

Installed with `uv sync --extra <name> ...`.

- `python` — `pyarrow`, `confluent-kafka`, `polars`; needed for the Python engine with Arrow Flight and Kafka reading
- `great-expectations` — feature validation with Expectation Suites
- `polars` — Polars DataFrame support
- `sqlalchemy-1` — legacy SQLAlchemy 1.x compatibility
- `mcp` — Model Context Protocol server (`hopsworks-mcp` script)
- `dev` — all of the above plus `pytest`, `ruff`, `docsig`, `pyspark`, `moto`, `delta-spark`, `pre-commit`

## Guarding Optional Imports

All optional-package code paths must be guarded.
Never import optional packages at module level.

```python
if TYPE_CHECKING:
    import polars

@uses_polars # from hopsworks_common.decorators
def to_polars_dataframe(self) -> polars.DataFrame:
    import polars
    ...
```

The `@uses_polars`, `@uses_great_expectations`, `@uses_confluent_kafka` decorators raise a clear `ImportError` with install instructions when the package is missing.

## Adding a New Dependency

- Core dep: add to `[project].dependencies` in `python/pyproject.toml`
- Optional dep: add to `[project.optional-dependencies]` and guard all imports
- Significant optional dep: add a corresponding test job in `.github/workflows/python.yml`

### Version Policy

- Strict pins for stability-critical packages: `pyhumps==1.6.1`, `avro==1.12.0`
- Range bounds for flexibility: `pandas<2.4.0`, `numpy>=1.26.3,<2.5.0`, `grpcio>=1.49.1,<2.0.0`
- A separate CI job tests with `pandas 1.x` to maintain backwards compatibility

## Documentation Links

- `requests` — https://docs.python-requests.org/en/latest/
- `furl` — https://raw.githubusercontent.com/gruns/furl/refs/heads/master/README.md
- `boto3` — https://docs.aws.amazon.com/boto3/latest/index.html
- `pandas` — https://pandas.pydata.org/docs/
- `numpy` — https://numpy.org/doc/stable/
- `pyhumps` — https://github.com/nficano/humps
- `avro` — https://avro.apache.org/docs/
- `grpcio` — https://grpc.io/docs/languages/python/
- `protobuf` — https://protobuf.dev/getting-started/pythontutorial/
- `opensearch-py` — https://opensearch-project.github.io/opensearch-py/
- `pyjks` — https://pyjks.readthedocs.io/en/latest/
- `hopsworks-apigen` — https://raw.githubusercontent.com/logicalclocks/hopsworks-apigen/refs/heads/main/README.md
- `fsspec` — https://filesystem-spec.readthedocs.io/en/latest/
- `retrying` — https://raw.githubusercontent.com/rholder/retrying/refs/heads/master/README.rst
- `pyarrow` — https://arrow.apache.org/docs/python/
- `confluent-kafka` — https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- `polars` — https://docs.pola.rs/
- `great-expectations` — https://docs.greatexpectations.io/

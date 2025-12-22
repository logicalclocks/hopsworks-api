# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the **Hopsworks Client SDK** repository, providing Python and Java APIs for interacting with Hopsworks Feature Store, Model Registry, and Model Serving. The repository is multi-language with separate but related Python and Java implementations.

## Repository Structure

### Python Packages (in `python/`)

- **`hopsworks`**: Main SDK for connecting to Hopsworks projects and accessing platform features
- **`hopsworks_common`**: Shared utilities, client implementations, and common code used across packages
- **`hsfs`**: Feature Store API - for creating/managing feature groups, feature views, and training datasets
- **`hsml`**: Machine Learning API - for model registry and model serving functionality

Each package follows a consistent internal structure:
- `client/`: REST API client implementations
- `core/`: Core business logic and metadata objects
- `engine/`: Execution engines (Python, Spark, etc.)

### Java Modules (in `java/`)

- **`hsfs`**: Core Feature Store library for JVM languages
- **`spark`**: Spark-specific Feature Store implementation
- **`flink`**: Flink-specific Feature Store implementation
- **`beam`**: Apache Beam support

The Java code supports multiple Spark versions via Maven profiles (e.g., `-Pspark-3.5`).

## Development Commands

### Python Development

**Environment Setup:**
```bash
cd python
uv sync --extra dev --all-groups
source .venv/bin/activate
```

**Running Tests:**
```bash
# All tests
pytest python/tests

# Specific test file
pytest python/tests/path/to/test_file.py

# With Avro support (if needed in Spark tests)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-avro_2.12:3.5.5 pyspark-shell"
pytest python/tests
```

**Linting and Formatting:**
```bash
cd python

# Run linter with auto-fix
ruff check --fix

# Format code
ruff format

# Check formatting without modifying
ruff format --check
```

**Pre-commit Hooks:**
```bash
# Install pre-commit hooks (run once)
pre-commit install

# Manually run pre-commit on all files
pre-commit run --all-files
```

**Check Aliases:**
```bash
# Verify API aliases are consistent
python python/aliases.py check
```

### Java Development

**Building:**
```bash
cd java

# Build with default Spark version
mvn clean package

# Build with specific Spark version
mvn clean package -Pspark-3.5

# Build with Hopsworks EE dependencies (requires credentials)
mvn clean package -Pspark-3.5,with-hops-ee
```

**Running Tests:**
```bash
cd java

# Run all tests
mvn clean test

# Run tests with specific Spark version
mvn clean test -Pspark-3.5
```

**Maven Credentials:**
To build Java code, you need Hopsworks Enterprise Edition repository access. Add to `~/.m2/settings.xml`:
```xml
<settings>
  <servers>
    <server>
      <id>HopsEE</id>
      <username>YOUR_NEXUS_USERNAME</username>
      <password>YOUR_NEXUS_PASSWORD</password>
    </server>
  </servers>
</settings>
```

### Documentation

**Building Documentation:**
```bash
# Generate API docs from docstrings
python python/auto_doc.py

# Serve docs locally (simple, without versioning)
mkdocs serve

# Build versioned docs with mike (preferred for production)
mike deploy 4.0.0-SNAPSHOT dev --update-alias
mike serve
```

**Documentation Versioning Scheme:**
- Development (master): `X.X.X-SNAPSHOT [dev]` with `dev` alias
- Latest stable release: `X.X.X [latest]` with `latest` alias
- Previous releases: `X.X.X` without alias

## Architecture Notes

### Python Client Architecture

The Python SDK operates in two modes:

1. **Python Mode**: Pure Python client for data science workflows, works in any Python environment (Jupyter, SageMaker, local, etc.). Used for exploring features, generating training datasets, and model serving.

2. **Spark Mode**: For data engineering jobs that write features to the Feature Store or generate large training datasets. Requires Spark environment (Hopsworks platform, Databricks, etc.). Provides both Python (PySpark) and Scala/Java APIs.

The architecture uses:
- **Client layer**: REST API communication with Hopsworks backend
- **Engine layer**: Abstracts execution environment (Python vs Spark)
- **Core layer**: Business logic and metadata objects that are engine-agnostic

### Java Client Architecture

Java clients are designed for Spark/Flink/Beam data engineering pipelines. They follow a similar pattern with:
- Core `hsfs` module with shared logic
- Engine-specific modules (`spark`, `flink`, `beam`) that extend the core

### API Consistency

The Python and Java APIs aim for consistency in method names and behavior. When modifying APIs, ensure changes are reflected in both languages where applicable.

## Testing Philosophy

- **Unit tests** mock external dependencies and test business logic
- Tests run against multiple Python versions (3.9-3.13) and multiple Spark versions
- Timezone-aware tests verify date/time handling in both UTC and local timezones
- Windows compatibility is tested separately
- Optional dependency tests ensure core functionality works without all extras installed

## Common Development Patterns

### Python Docstring Format

Use Google-style docstrings with specific sections:
```python
"""[One Line Summary]

[Extended Summary]

!!! example
    ```python
    import example
    ```

# Arguments
    arg1: Type[, optional]. Description[, defaults to `default`]
    arg2: Type[, optional]. Description[, defaults to `default`]

# Returns
    Type. Description.

# Raises
    Exception. Description.
"""
```

### Docstring Requirements by Component

- **Public API methods**: Fully documented with all sections and defaults
- **Engine methods** (e.g., ExecutionEngine): Single line docstring only
- **Private REST APIs** (e.g., FeatureGroupApi): Fully documented without defaults
- **Public REST APIs**: Fully documented with defaults

### Adding New API Documentation

1. Add entry to `python/auto_doc.py`:
   ```python
   PAGES = {
       "new_api.md": [
           "module.path.ClassName.method_name"
       ]
   }
   ```

2. Create template in `docs/templates/new_api.md` with placeholder tags:
   ```markdown
   {{module.path.ClassName.method_name}}
   ```

3. Run `python python/auto_doc.py` to generate docs

## Supported Versions

- **Python**: 3.9 - 3.13
- **Java**: JDK 8+ (compiled with JDK 8 for compatibility)
- **Spark**: 3.5.5 (primary), with profiles for other versions
- **Pandas**: Both 1.x and 2.x are supported and tested

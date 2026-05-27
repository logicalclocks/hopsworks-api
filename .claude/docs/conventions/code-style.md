# Code Style

## Naming

- Classes: `PascalCase` — `FeatureGroup`, `StorageConnector`
- Functions and methods: `snake_case` — `get_feature_view`, `insert`
- Private attributes: `_leading_underscore` — `_feature_store_id`, `_api_key`
- Constants: `SCREAMING_SNAKE_CASE` — `HOPSWORKS_PORT`, `DEFAULT_VERSION`
- Module-level logger: always `_logger = logging.getLogger(__name__)`

## Imports

Order (enforced by Ruff):

```python
from __future__ import annotations  # always first if present

import json  # stdlib
import logging
from typing import TYPE_CHECKING

import pandas as pd  # third-party

from hopsworks_common.client import exceptions  # local
from hsfs.feature import Feature

if TYPE_CHECKING:
    from hsfs.feature_store import FeatureStore  # annotation-only
```

- `from __future__ import annotations` goes first when the file uses forward references
- Annotation-only imports belong in `if TYPE_CHECKING:` blocks — avoids circular imports at runtime
- No wildcard imports

## Type Hints

Always annotate public function signatures.
Use `X | Y` instead of `Union[X, Y]` (requires `from __future__ import annotations`).
Explicit `-> None` on void methods.

```python
def get_feature(
    self,
    name: str,
    version: int | None = None,
) -> Feature | None:
    ...
```

## Error Handling

- Use `RestAPIError` for HTTP errors from the cluster; it has `.status_code` and `.error_code`
- Use `FeatureStoreException` for logic errors within the Feature Store client
- Do not catch broad `Exception` unless re-raising or explicitly recovering
- Use `@catch_not_found` from `hopsworks_common.decorators` for methods that should return `None` on 404:

```python
@catch_not_found()
def get_feature_group(self, name: str, version: int) -> FeatureGroup | None:
    return self._feature_group_api.get(name, version)
```

- Error messages must identify the resource and operation; non-obvious cases should suggest recovery

## Logging

```python
import logging
_logger = logging.getLogger(__name__)

_logger.info("Inserting %d rows into feature group '%s'.", len(df), self._name)
_logger.warning("Feature group '%s' is missing statistics config.", self._name)
```

- No `print()` in library code
- No logging of sensitive data (API keys, credentials, PII)
- Use `%s` formatting in log calls, not f-strings, to defer string construction

## JSON Serialization

Domain objects implement `.to_dict()` returning a JSON-serializable dict, and a `from_response_json(dict)` class method for deserialization.
`hopsworks_common.util.Encoder` calls `.to_dict()` automatically.

## Engine Dispatch

The execution engine (Python vs Spark) is selected at connection time.
Domain objects do not check the engine type.
Engine selection is handled by `core/<entity>_engine.py` via `engine.get_instance()`.
Do not put `if engine == "spark": ...` logic in domain objects.

# Generated aliases break pytest run from source

`hopsworks-apigen` synthesizes shim modules at build time so the same class can be imported under several namespaces (for example `hopsworks_common.decorators.catch_not_found` is also reachable as `hsml.decorators.catch_not_found`, `hsfs.decorators.catch_not_found`, `hopsworks.decorators.catch_not_found`).
The shim files live at paths like `python/hsml/decorators/__init__.py`, `python/hsfs/core/variable_api/__init__.py`, etc.
Those `__init__.py` files are **gitignored**; only the empty package directories sit in source.

When you run `pytest python/tests/...` directly against an unbuilt source tree, the shims are absent and tests fail with messages like:

```
ImportError: cannot import name 'VariableApi' from 'hsfs.core.variable_api' (unknown location)
AttributeError: module 'hsml.decorators' has no attribute 'catch_not_found'
```

These look like bugs in the test or in the package; they are not.

## Fix

Build the package once before running pytest so the shims get generated.
The canonical path is `uv sync --extra dev --all-groups --project python`, which builds `hopsworks` and populates the shim trees.
If your test environment cannot run `uv sync` (no network, mismatched Python, etc.), install the project into a venv and copy the generated shim `__init__.py` files from `site-packages/` back into the source tree:

```bash
for pkg in hsml/decorators hopsworks/decorators hsfs/decorators \
           hsfs/core/variable_api; do
  cp <venv>/lib/python3.13/site-packages/$pkg/__init__.py python/$pkg/__init__.py
done
```

The shim list grows over time, so prefer `uv sync` when possible.

## Don't commit the shims

`.gitignore` already excludes them, but the empty package directories live in git so the source tree compiles a wheel correctly.
If you ever see a diff that adds `python/hsml/decorators/__init__.py` (or similar) to source control, drop it; that file is regenerated on every build and committing one will go stale immediately.

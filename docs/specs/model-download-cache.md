# Model Download Cache — Specification

Status: implemented
Scope: `hsml.engine.model_engine.ModelEngine.download`, `hsml.model.Model.download`, `hsml.model.Model.clear_cache`

## Summary

`Model.download()` caches downloaded model files on the local filesystem and
reuses them across calls and across processes, instead of downloading into the
current working directory every time.
The cache is keyed by the backend model `id`, so a model version that is
deleted and recreated is re-downloaded automatically — callers never have to
invalidate the cache manually.
Downloads fall back across multiple filesystem locations so that a full disk,
a read-only filesystem, or a permission problem in one location does not fail
the download outright.

## Cache locations

When `local_path` is not given, the download is attempted in the following base
directories, in order, falling back to the next when one is unusable:

1. System temp area — `MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT` (`/tmp/hopsworks/models` by default)
2. Hopsworks home cache — `~/.hopsworks/cache/models`
3. Working directory — `<cwd>/.hopsworks_cache/models`

The list is de-duplicated while preserving order (e.g. when the working
directory resolves under the temp base), and every entry is made absolute.

### Per-model path layout

Within a base directory the cache path is:

```
{base}/{project_name}/{model_name}/{version}/{id}
```

- `project_name` is included so that two projects (e.g. a shared registry) with
  a model of the same name and version never collide.
- `id` is the backend model primary key. It is the leaf segment so that a
  recreated model version (same `project/name/version`, new `id`) resolves to a
  different directory and is downloaded fresh.

If `project_name` or `id` is `None`, caching cannot be keyed safely; the caller
must pass an explicit `local_path` instead (a `ValueError` is raised otherwise).

## Behaviour

### Reuse

A cache directory is reused only when all of the following hold:

- the directory exists and is non-empty,
- it is owned by the current OS user (see Security), and
- it contains a `.download_complete` marker file.

On reuse, a message is printed indicating that cached files are being used and
how to force a fresh download (pass `local_path` or call `Model.clear_cache`).

### Automatic invalidation on recreation

Because the path is keyed by `id`, a recreated model version simply does not
match the previous directory, so it is re-downloaded with no user action.
After a successful download, sibling directories for other `id`s of the same
`version` are pruned (best-effort, owner-only) so the cache does not grow each
time a version is recreated.

### Fresh download and fallback

When no valid cache exists, the model is downloaded into the first base
directory that works. A directory is `OSError`-classified and the next base is
tried when:

- disk is full (`ENOSPC`),
- permission is denied (`EACCES` / `EPERM`),
- the filesystem is read-only (`EROFS`).

For each failure an actionable message is printed (free space, fix
ownership/permissions with an `ls -ld` hint, or use a writable filesystem) and
states whether a fallback location will be tried.
If every location fails, the last error is raised.
Before falling back, any partial download we own is removed so it cannot later
look like a complete cache.

### Explicit `local_path`

When the caller passes `local_path`, it is honoured exactly: no cache lookup,
no fallback, and pre-existing files in that directory are preserved (the
directory is not cleaned). Errors are reported with the same actionable
messages and then raised.

### Completion marker

A `.download_complete` marker is written only after a cache download finishes,
and is what distinguishes a complete cache from a partial one. Explicit
`local_path` downloads do not write a marker.

## Security

The temp cache path under `/tmp` is world-readable and predictable, so on a
multi-user host another local user could pre-create it. To prevent reuse of, or
writes into, attacker-controlled directories:

- A cache directory is trusted (reused) only if it is owned by the current user.
- Pre-existing cache directories owned by another user are never reused,
  cleaned, or overwritten; the download falls back to a location the user owns
  (e.g. `~/.hopsworks/cache/models`).
- Cache directories are created with owner-only `0o700` permissions.
- Cleanup and pruning never delete directories owned by another user.

On platforms without POSIX ownership (e.g. Windows) the ownership check is a
no-op (treated as owned).

## `Model.clear_cache`

`Model.clear_cache(project_name=None, model_name=None, version=None) -> int`
removes cached models from every base location and returns the number of model
versions removed.

Filters narrow from broad to specific and are validated before any filesystem
access:

- `model_name` requires `project_name`.
- `version` requires both `project_name` and `model_name`.

Invalid combinations raise `ValueError` and delete nothing. With automatic
invalidation in place, `clear_cache` is normally only needed to reclaim disk
space, not to fix stale models.

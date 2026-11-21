# Semantic layer tags

Semantic projects and metrics carry schematised tags, as feature groups and jobs do; an administrator defines the tag schemas, and Hopsworks Enterprise is needed.

```bash
hops semantic tag set sales ownership '{"owner": "finance", "certified": true}'
hops semantic tag list sales
hops semantic tag get sales ownership
hops semantic tag delete sales ownership
hops search --type metric --tag ownership:owner=finance        # tag filters work on metrics and semantic projects
```

```python
sales.add_tag("ownership", {"owner": "finance"})
sales.get_tags()                                   # {"ownership": {"owner": "finance"}}
sl.create_project("sales", "Resources/semantic/sales", tags={"ownership": {"owner": "finance"}})
```

A metric's tags live in its definition, under `config.meta.hopsworks.tags`, tag schema name to value, so they change with the YAML and its git history:

```yaml
metrics:
  - name: revenue
    type: simple
    type_params: {measure: revenue}
    config:
      meta:
        hopsworks:
          tags:
            ownership: {"owner": "finance", "certified": true}
            domain: "sales"
```

- A compile checks every metric's tags against the tag schemas; an unknown schema (`METRIC_TAG_UNKNOWN_SCHEMA`), a value that does not match it (`METRIC_TAG_INVALID_VALUE`) or a missing mandatory metric tag (`METRIC_TAG_MANDATORY_MISSING`) is an error at the metric's file, so the build is `INVALID` and the last valid build keeps serving.
- Mandatory tags for semantic projects are checked on create: pass them with `--tag` or `tags=`.
- Only metrics take tags in the YAML; semantic models, dimensions and entities do not.

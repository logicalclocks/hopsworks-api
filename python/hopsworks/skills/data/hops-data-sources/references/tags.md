# Tag a data source

Schematized tags classify a data source (owner, source system, sensitivity) and make it findable in search.
The tag is stored on the storage connector, so every `DataSource` of that connector shows the same tags.
The tag schema must exist first (an admin defines it in the cluster settings); the value is validated against it.
Only a data owner of the project that owns the feature store can attach or delete tags; the online feature store connectors cannot be tagged.

```python
ds = fs.get_data_source("my_connector")
ds.add_tag("data_owner", {"team": "risk", "pii": True})   # replaces an existing value
ds.get_tag("data_owner")                                   # value, or None when not attached
ds.get_tags()                                              # {name: value}
ds.get_tag_metadata("data_owner").created_on               # attachment time
ds.delete_tag("data_owner")
```

Find tagged data sources with `hops search --type data_source --tag data_owner:team=risk`.

## Mandatory tags

An admin can make a tag schema mandatory for data sources, cluster-wide or for one project.
A data source is then created only with that tag, passed in the create call:

```python
sc.save(tags=[{"name": "data_owner", "value": {"team": "risk"}}])
```

`fs.create_data_source(sc, tags=[...])` takes the same `tags`, in place of `save()`.
`hops datasource create` cannot attach tags, so it fails while a tag is mandatory; create such a data source from Python.

A data source created before the tag became mandatory reports it in `missing_mandatory_tags`, and fetching it warns.
Updating such a data source is refused until the tag is added with `add_tag`.

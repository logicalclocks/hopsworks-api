## Mount an Elasticsearch index

Elasticsearch has no databases: `get_tables()` lists every open index, alias and data stream, hidden and system ones excluded.
Fields become features named by their lowercased path with dots as underscores, so `user.name` is `user_name` and `@timestamp` is `timestamp`; two paths that collide are refused.
Object fields are flattened, while `nested`, `geo_*` and other structured fields arrive as JSON strings.
A field holding arrays becomes `array<...>` when the preview sees a list; a list arriving later in a column the feature group declares scalar fails the read and names the document, so recreate the feature group with the array type.

```python
ds = fs.get_data_source("my_elasticsearch")
index = [t for t in ds.get_tables() if t.table == "events"][0]
ext_fg = fs.create_external_feature_group(
    name="events", version=1, data_source=index, primary_key=["event_id"], event_time="timestamp"
)
ext_fg.save()
df = ext_fg.read()   # Python engine: via the Hopsworks Query Service
```

Set `data_source.query` to a query DSL document (a bare clause or `{"query": ...}`) to mount a subset of the index named by `data_source.table`.
Filters on the feature group are pushed down as an Elasticsearch bool query.
`ds.storage_connector.read("events")` and external feature group reads on the Spark engine need the `elasticsearch-spark-40_2.13` jar on the Spark classpath, and array fields listed in the `es.read.field.as.array.include` argument.
On Spark, `verify=False` accepts only a single self-signed certificate; a cluster whose certificate chains to a private CA needs a truststore.
`ds.storage_connector.connector_options()` returns keyword arguments for the official `elasticsearch` client, which is not a Hopsworks dependency.

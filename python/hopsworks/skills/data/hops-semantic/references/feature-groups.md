# Semantic models and their feature groups

`model: ref('x')` resolves to a feature group, in this order: `config.meta.hopsworks.feature_group` and `version` on the semantic model, then the feature group named `x` at its latest version.

Use the `meta` override whenever the dbt model and the feature group have different names, or to pin a version so a new FG version does not silently change the metric.
A feature group in another project's feature store, shared with this one, also needs `feature_store: <project>_featurestore` in that `meta`.
In MetricFlow's native format, `node_relation` names the FG table as `<project>_featurestore.<fg>_<version>`.
Cached and stream feature groups of the project, or shared with it, resolve through their offline table.
An external feature group over a table of a data source resolves through a Trino catalog of the project created from that data source, once Trino serves it (`sc.create_trino_catalog()` on the data source, by a Data Owner, then a Trino restart): a project catalog, not a private one.
An external feature group over a SQL query or a path, a spine group, and any other table do not resolve.
Every `expr` is checked against the feature group's columns at compile time.
The compiler links the FG `event_time` to the model's time dimension on that column, adding one at `day` grain named after the column when none is declared, and a measure with no `agg_time_dimension`, its own or in `defaults`, aggregates over it.
Declare the time dimension for a finer grain, and set `agg_time_dimension` only to aggregate over another time; a FG without `event_time` needs it, else `AGG_TIME_DIMENSION`, and an `EVENT_TIME` warning means the link clashed with another element's name.
Nothing checks that a primary entity is the FG primary key, so match it yourself: joins trust a primary entity to be unique.

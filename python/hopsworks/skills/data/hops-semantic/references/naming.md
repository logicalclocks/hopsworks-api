# Names a compile accepts

The name rules of MetricFlow's validator, which `hops semantic compile` enforces; each issue names its rule.
Names of semantic models, entities, dimensions, measures, metrics and custom granularities are lower-cased first; saved query names are checked as written.

| Rule | Applies to | A valid name |
|---|---|---|
| `NAMING` | every name, aliases included | lower case letters, digits and underscores; starts with a letter, ends with a letter or digit, at least two characters, no `__` (the group-by separator) |
| `NAMING` | every name | is not a time grain (`nanosecond` to `year`: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, ...), `metric_time` or `mf_internal_uuid` |
| `RESERVED_KEYWORD` | entities, dimensions, measures, and `node_relation` schema and alias | is not one of `and as create distinct for from full having in inner into is join left like natural not null on or right select union using where with` |
| `UNIQUE_NAMES` | every name | in a semantic model, one name is one entity, dimension or measure; a measure name is used in one semantic model only; metric, semantic model and saved query names are unique |
| `ELEMENT_CONSISTENCY` | across semantic models | a name is the same kind of element everywhere: `customer` cannot be an entity in one model and a dimension in another |
| `DIMENSION_CONSISTENCY` | dimensions of one name | have one type, time or categorical, and are all partitions or none |
| `PRIMARY_ENTITY_DIMENSION` | models with the same primary entity | do not share a dimension name, since `<entity>__<dimension>` would name both |
| `LABELS` | labels | a metric or semantic model label is unique; in a model, dimension, entity and measure labels are unique |

The rules apply to the element's `name` only: a feature called `day` or `from` is still read through `expr`, quoted when the expression parser takes it for a keyword:

```yaml
dimensions:
  - name: event_day          # not `day`, a time grain
    type: time
    expr: '"day"'
    type_params:
      time_granularity: day
  - name: origin             # not `from`, an SQL keyword
    type: categorical
    expr: '"from"'
```

`hops semantic init` writes names that pass: a column whose name the rules refuse gets a `_value` suffix (`day` becomes `day_value`, a one-letter `x` becomes `x_value`), and a name already in use, here or as another kind of element in another model, gets the model name as a prefix ([init.md](init.md)).

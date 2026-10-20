# A table for a question

A table made to answer a question ("where does revenue come from?") has no target and no signal
columns: its story is what the question should find (what grows over time, which category or
country dominates, a seasonal peak). One generator `./data/<name>/generate.py`, run once where the
agent runs, writes one offline Delta feature group. Nothing about targets, prevalence,
`system.yaml`, the bundle or live mode applies.

## Code rules

These hold for every generator of this skill, an ML system's too.

- **Seeded.** The same seed gives the same data on any cluster; a live tick is
  seeded from its index, so a restart continues from now with no state.
- **Polars, and as a job in `python-feature-pipeline`.** The columns and the
  relational steps that shape the story (joins between entities and events,
  window aggregates) are Polars; the job base ships it, so a generator run as a
  job needs no cloned environment. Never a Spark job: Polars generates the small
  tier's history in seconds.
- **Whole columns, never rows.** Draw each random column in one call on a
  seeded `numpy.random.default_rng` (`choice`, `normal`, `gamma`, `poisson`,
  `uniform`), then build the rest with Polars expressions: ids with
  `pl.format("c-{}", pl.int_range(pl.len()))`, times with
  `pl.lit(start) + pl.duration(microseconds=...)` or `pl.datetime_range`,
  snapshots by a cross join with the date grid, labels and story effects with
  `pl.when`, joins and window expressions. No Faker, no Python loop or list
  comprehension over rows, no `map_elements`, `apply` or `iter_rows`: each
  costs a Python call per row, about ten times slower than the whole column at a
  million rows. Names, cities and products come from a short list sampled with
  `rng.choice`.
- **Statistics off.** Every feature group the generator writes is created
  with `statistics_config=False`: otherwise Hopsworks computes statistics in a
  PySpark job after every insert. The job `<fg>_<version>_compute_stats` still
  exists for every feature group; with statistics off it never runs.

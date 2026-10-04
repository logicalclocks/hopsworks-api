# Gold layers and data marts

A gold layer serves analysts: consumption-ready tables in a Kimball dimensional model, star or snowflake schema, built from silver tables.
It is organised as data marts, each serving one business process for one group of analysts, and each added, changed and deleted on its own, with its own jobs at its own cadence.
One gold layer is one directory and one GitHub repository, `hops-<slug>`; every mart's code lives in it.

## The data mart's requirements

The Factory's form asks these questions and `system.yaml` records the answers under `marts[].requirements`; `/hops-gold` asks the ones left blank, one `AskUserQuestion` call at a time, before designing anything.

| Key | Question |
| --- | --- |
| `analysts` | Who are the analysts this mart serves? |
| `decisions` | What decisions and reports will it support? |
| `example_queries` | Example queries, with the results they expect. |
| `approver` | Who approves the business definitions (metrics, grain, filters)? |
| `existing_tables` | Do suitable facts or dimensions already exist? The build lists the gold tables of every mart in the project and suggests reusing or extending them before creating new ones from silver. |
| `grain.represents` | What exactly does one row represent? |
| `grain.identifier` | What uniquely identifies a row? |
| `grain.type` | `transaction` (individual events), `periodic_snapshot`, or `accumulating_snapshot`, or an aggregate of one of these. |
| `metrics` | Each metric's exact formula, exclusions, filters, currency and unit. |
| `cadence`, `freshness_hours` | How often the mart is refreshed, and how stale it may be. |
| `late_data` | How late arrivals, updates and deletes from silver are processed. |
| `restate` | Whether a correction restates results already published, or only changes results from now on. |
| `reconcile` | Which totals must reconcile with existing reports or source systems. |
| `invariants` | The invariants and edge cases the tests must cover. |
| `on_check_failure` | `fail` the run and publish nothing, `quarantine` the failing rows, or `warn` and publish. |
| `access` | Who may read which rows and columns. |
| `share` | Whether the mart is shared with other projects, and which. |

## Modeling

- **Facts** hold measurements at the declared grain, keyed by the dimensions' surrogate keys and the event or snapshot time; additive measures are stored, ratios are computed from stored numerators and denominators.
- **Dimensions** hold descriptive attributes, with a surrogate key; slowly changing attributes the analysts filter history by are type 2 (`valid_from`, `valid_to`, `is_current`), the rest type 1.
- **Star schema**: each dimension is one denormalized table. **Snowflake schema**: dimensions normalized into their hierarchies (product to category), as the layer's `modeling` says.
- **Conformed dimensions** (customer, product, date) are built once in the layer and shared: a mart that needs one reads it and lists it with `shared: true`.
- A date dimension is generated, not derived from the data.

## Standards

Every mart follows the layer's `standards`; the Factory proposes these, and the user may edit them:

- **Naming**: `fct_<process>` for facts, `dim_<entity>` for dimensions, `agg_<process>_<grain>` for aggregates, snake case, the business's terms; never the name of an existing feature group.
- **Modeling**: one declared grain per fact; surrogate keys on every dimension; no measure without a unit; no nulls in foreign keys (an "unknown" dimension member instead).
- **Documentation**: every table and column has a description, every metric its formula in the feature group description, and the mart's `README.md` in the repository lists its tables, grain, metrics, owner and approver.
- **Quality**: dbt tests (or Great Expectations) for keys, not-null foreign keys, accepted values and the mart's invariants; the reconciliation checks run on every refresh; a check failure is handled as `on_check_failure` says.

## Jobs

A mart has one job per cadence it needs, `<slug>-<mart>-<cadence>`, running one program (dbt on Trino by default) that builds that mart's tables for the window, scheduled with catch-up, and reading its silver sources incrementally as the silver layer does.
Each job is listed in the mart's `jobs` with the tables it writes, so a job, and the tables only it writes, can be deleted on their own.

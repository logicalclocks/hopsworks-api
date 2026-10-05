# Gold layers and data marts

A gold layer serves analysts: consumption-ready tables in a Kimball dimensional model, star or snowflake schema, built from silver tables.
It is organised as data marts, each serving one business process for one group of analysts, and each added, changed and deleted on its own, with its own jobs at its own cadence.
One gold layer is one directory, in the GitHub repository it shares with the silver layer it reads (`layer.repo`); every mart's code lives in its directory.

## The data mart's requirements

The Factory's form asks these questions and `system.yaml` records the answers under `marts[].requirements`; `/hops-gold` asks the ones left blank, one `AskUserQuestion` call at a time, before designing anything.

| Key | Question |
| --- | --- |
| `analysts` | Who are the analysts this mart serves? |
| `decisions` | What decisions and reports will it support? |
| `approver` | Who approves the business definitions (metrics, grain, filters)? |
| `existing_tables` | Do suitable facts or dimensions already exist? The build lists the gold tables of every mart in the project and suggests reusing or extending them before creating new ones from silver. |
| `grain.represents` | What exactly does one row represent? |
| `grain.identifier` | What uniquely identifies a row? |
| `grain.type` | `transaction` (individual events), `periodic_snapshot`, or `accumulating_snapshot`, or an aggregate of one of these. |
| `metrics` | Each metric's exact formula, exclusions, filters, currency and unit. |
| `cadence`, `freshness_hours` | How often the mart is refreshed, and how stale it may be. |
| `late_data` | How late arrivals, updates and deletes from silver are processed. |
| `restate` | Whether a correction restates results already published, or only changes results from now on. |
| `invariants` | The invariants and edge cases the tests must cover. |
| `on_check_failure` | `fail` the run and publish nothing, `quarantine` the failing rows, or `warn` and publish. |
| `access` | Who may read which rows and columns. |
| `share` | Whether the mart is shared with other projects, and which. |

### Verification: how the user knows the mart is right

These three are the mart's acceptance tests, and the mart is not `built` until each one passes on the cluster.

| Key | Question |
| --- | --- |
| `example_queries` | Example questions with the answers the user expects, in plain English ("revenue in Europe in the first week of 2024 is about 1.2M EUR"). The build turns each into SQL over the mart and checks the answer. |
| `reconcile` | Totals that must reconcile, and with what. Suggestions: the fact's row count equals the silver rows at its grain for the same window; each additive measure's total equals the silver total it is computed from; distinct dimension keys equal the distinct silver entities; a monthly total is within a stated tolerance of an existing report or source system. |
| `refresh_checks` | Proof that refreshes and reruns behave. Suggestions: rerunning a window leaves every table unchanged; a refresh writes only the new window's rows; a late-arriving silver row updates the period it belongs to (or not, as `restate` says); a backfill over the whole history equals the sum of incremental runs. |

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

# hops semantic init

What `hops semantic init` writes and the flags it takes; the parent skill's "Start a project" section has the commands.

- Each feature group becomes a semantic model pinned to its version (`config.meta.hopsworks.feature_group` and `version`): its primary key is the primary entity, a column named like another model's single-column key a foreign entity, its `event_time` the time dimension (day grain) behind `metric_time`, each string feature a categorical dimension, and a row count and a sum per numeric feature its measures.
- A feature group without an `event_time` gets dimensions but no measures, since every measure needs a time dimension; it still serves as a dimension table that other models join to.
- `--repo new` needs `gh auth status` to pass (`github-login` in a Hopsworks terminal). It creates `hops-semantic-<name>`, or the next free name, pushes the scaffold, clones it into `Resources/semantic/<repository>`, and registers the clone, so a pull recompiles and each build records its commit. Hopsworks clones with the GitHub token in your account (`hops git provider list`); when there is none, init registers `gh auth token` after asking. `--yes` answers both questions.
- `--path` moves the scaffold; `--mode PINNED` and `--tag NAME=VALUE` work as on `hops semantic create`.

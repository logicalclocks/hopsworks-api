# ML systems built from system.yaml

Each directory in this repository that holds a `system.yaml` is an ML system built on Hopsworks with Claude Code, by `/hops-build <slug>` (Brewer in the Hopsworks UI).
`system.yaml` is the specification: the requirements, the data sources, the feature pipelines, training, inference and the app.
The code in the directory, its jobs and schedules, and the Hopsworks assets it creates (feature groups, feature views, training datasets, models, deployments, apps) are built from it, and must stay in step with it.

## When system.yaml has changed

Before anything else in a session, check whether `system.yaml` changed since what is built was last recorded, and what that means for the system.

- Edits made in the Hopsworks UI are listed under `system.edits`, each with the paths it changed, and each is one commit with a `Brewer-Edit: <id>` trailer: `git log --grep "Brewer-Edit" -p -- <slug>/system.yaml`.
  Apply them with `/hops-build <slug> <phase> Apply the edits ...`, which rebuilds what they change and removes the entries.
- Edits made any other way: `git log -p -- <slug>/system.yaml` since the phase's last commit (`[<slug>] <phase>: ...`).

For each change, decide which pipelines or assets it touches: a schedule means rescheduling the job, a feature means changing the feature pipeline and usually a new feature group version, a model or split setting means retraining, an SLA or window means the inference job or deployment, and so on.
Change what is needed, record it in `system.yaml`, and leave the rest untouched.

## What a change affects downstream

A component you change can change or break everything that reads from it, and those components may need to be re-implemented and redeployed too.
Look at a component's lineage with the hops CLI before changing it, and at its downstream components after:

```bash
hops fg lineage <feature_group> [--version N]    # parents and storage connector; downstream feature views and groups
hops fv lineage <feature_view> [--version N]     # the feature groups it reads; its training datasets and models
hops td lineage <feature_view> [version]         # a training dataset's feature view and models
hops model lineage <model> [--version N]         # a model's feature view and training dataset
hops deployment lineage <deployment>             # the model a deployment serves and that model's lineage
```

Follow the chain from what changed: a new feature group version needs the feature views that join it, their training datasets, the models trained on them, the batch jobs or deployments that serve those models, and the app that reads the predictions.
Rebuild each affected component in that order, record it in `system.yaml`, and never delete a version something downstream still reads.

# An ML system built from system.yaml

This directory is an ML system built on Hopsworks with Claude Code, by `/hops-build <slug>` (Factory in the Hopsworks UI), where `<slug>` is this directory's name.
`system.yaml` is the specification: the requirements, the data sources, the feature pipelines, training, inference and the app.
The code here, its jobs and schedules, and the Hopsworks assets it creates (feature groups, feature views, training datasets, models, deployments, apps) are built from it, and must stay in step with it.
Paths the hops skills write as `<slug>/<path>` are `<path>` in this directory.

## system.yaml always describes the system as it is

Every change you make to this ML system updates `system.yaml` in the same commit, whatever it is for: a fix, maintenance, a retrain, a new version of an asset, a changed schedule, memory, cores or replicas, a pinned library, a job or asset added or removed.
Record it in the block of the phase it belongs to (names, versions, settings, status), and say why in `decisions`.
A change made outside the build, such as a fix you were asked for in a session or one suggested from the Factory's status page, is no exception.
`system.yaml` must always reflect the current state of the ML system: before you finish, check that every asset, version and setting it names is the one that runs, and correct it where it is not.

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory or any other git work tree: logs are not checked in.

- Jobs and deployments the factory runs log to stdout and stderr, which Hopsworks archives in the project's `Logs` dataset (`/Projects/<project>/Logs`); never redirect them into a file here.
- Programs you run in the terminal as part of the factory (a local test or benchmark, a `nohup` or `tee` of a long command) write their logs to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`; create it with `mkdir -p` first.
- Read a job's logs with `hops job logs <job> --stdout --tail 200`, which leaves no files, or download them with `--dir ${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>`, never into the working directory; `hops deployment logs <name> --download` and `hops agent logs <name> --download` take the same `--dir`.
- `execution.download_logs()` and `deployment.download_logs()` in Python write to the working directory unless given `path=`, so pass that directory or a temporary one.
- A `logs-job-*`, `logs-deployment-*` or `*.log` entry in this directory is a mistake: move it to `Logs/factory/<slug>/` and keep it out of commits.

## When system.yaml has changed

Before anything else in a session, check whether `system.yaml` changed since what is built was last recorded, and what that means for the system.

- Edits made in the Hopsworks UI are listed under `system.edits`, each with the paths it changed, and each is one commit with a `Brewer-Edit: <id>` trailer: `git log --grep "Brewer-Edit" -p -- system.yaml`.
  Apply them with `/hops-build <slug> <phase> Apply the edits ...`, which rebuilds what they change and removes the entries.
- Edits made any other way: `git log -p -- system.yaml` since the phase's last commit (`[<slug>] <phase>: ...`).
- Each applied set of edits is one commit with a `Brewer-Apply: <ids>` trailer whose body lists every asset as `<name>: v<old> -> v<new>` (`git log --grep "Brewer-Apply"`).
  The previous versions are kept, so reverting that commit and applying again returns the system to them.

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

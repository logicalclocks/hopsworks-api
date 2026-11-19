# /hops-build: edits from the Hopsworks UI

Part of `/hops-build` (`hops-build.md`), read at the step that names it; the Rules and the lock there still hold.

`system.edits` lists changes a user made to `system.yaml` from the Hopsworks UI (the Factory
page's system architecture), not yet applied. Each is one commit, `[<slug>] edit: <box> in
system.yaml` with a `Brewer-Edit: <id>` trailer, and one entry: `{id, paths, box, phase, at, by}`,
where `paths` are the dotted paths it changed (`features.pipelines[0]`, `data.customers`). The UI
shows the phase as edited and offers Apply changes, which starts
`/hops-build <slug> <phase> Apply the edits made in the Hopsworks UI ...`, and Revert, which
`git revert`s those commits. Applying: the edit is the new specification for those paths, so the
named phase and every later phase it invalidates are rebuilt as for any `/hops-build <phase>
<instruction>` change (an edit that breaks a requirement is a requirements change and is asked
about first); then delete the applied entries from `system.edits` with `set.py` and record the
phase `met`, or `unmet` with why. Never apply an edit that is not listed, and never revert one.

**Applying cascades.** First mark `stale`, by the transition table in `system-yaml.md`, the edited
phase and every later phase it invalidates, and commit that. Then rebuild them in order, each
from the new outputs of the one before, never reading the old versions: an edit to `data`
(regenerated synthetic data, a new source) writes new feature group versions, reruns every feature
pipeline into new versions, recreates the feature view and its training data, retrains and
accepts a model, reruns batch inference or redeploys the deployment or agent, points the app at
the new versions and restarts it, and runs `verify`. Rewrite each later phase's block in
`system.yaml` and its code to the new names and versions, rerun its tests, and record it `met`;
the run ends only when every invalidated phase is `met`, `accepted` or `unmet` with why.

**One commit per apply, and the old versions kept.** Keep every superseded version (feature
groups, feature views, training datasets, models): an apply is undone by reverting its commit,
which only works while what the previous `system.yaml` names still exists. Stop only what would
run twice: unschedule and stop the old pipelines' jobs, and move the deployment and app to the new
versions. End the apply with one commit of `system.yaml` and the code,
`[<slug>] apply: <boxes>`, with a `Brewer-Apply: <edit ids>` trailer and a body listing each asset
as `<name>: v<old> -> v<new>`, pushed. To go back, `git revert --no-edit <that commit>` restores
the previous `system.yaml` and code; then apply it like any edit, which re-points the jobs,
schedules, deployment and app at the previous versions without rebuilding them, and commits that
as its own apply. Delete removes every version a system made, the superseded ones included.
An apply ends like any build, with `verify` and a release: `minor` for a changed specification,
`patch` for a revert to a released version's specification (repo.md, **Releases**). The first
change after a release sets `system.version` to that next version in the same commit.

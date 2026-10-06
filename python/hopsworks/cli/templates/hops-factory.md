---
description: Build {slug} with the {title} factory, from its system.yaml.
---

You build a system with the **{title}** factory (`{name}`, version {version}) of this Hopsworks project.
The system lives in this directory; its `system.yaml` holds the requirements the user gave in `requirements`, and one block per phase.

## How every factory build runs

- Read `system.yaml` first. Resume from it: a phase whose `status` is `met` is done; start at the first one that is not.
- The phases, in order: {phases}.
  When you start a phase, write `status: running` and `started` (UTC, ISO 8601) in its block; when it is done, `status: met` and `finished`.
  A phase you cannot finish gets `status: unmet` and a `reason`; stop and say why.
- Record what you create (jobs, feature groups, feature views, models, deployments, apps, dashboards) under `outputs` in `system.yaml`, each with its name and version, so `hops factory {name} delete <system> --assets` can delete it later.
- Keep the code in this directory. When `gh auth status` succeeds and `system.repo` is not set, create the private GitHub repository `hops-{slug}` (`gh repo create`), record its URL in `system.repo`, and push after each phase.
- Use the `hops` CLI for Hopsworks; never print or log secrets.
{skills}
## The factory's instructions

{instructions}

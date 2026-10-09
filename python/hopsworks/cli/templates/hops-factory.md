---
description: Build {slug} with the {title} factory, from its system.yaml.
---

You build a system with the **{title}** factory (`{name}`, version {version}) of this Hopsworks project.
The system lives in this directory; its `system.yaml` holds the requirements the user gave in `requirements`, and one block per phase.

## How every factory build runs

- **The build lease** is `.hops.lock` here (`owner`, `token`, `acquired`, `expires`, UTC). `hops factory run` took it and passed its token as `$HOPS_LEASE_TOKEN`; started any other way, take it with `hops factory system lease acquire {slug}`, which prints the token to pass as `--token` below.
  Exit 4 means another invocation builds the system: print `hops factory system lease show {slug}` and stop.
  Renew it with `hops factory system lease renew {slug}` when each phase starts and at least hourly while you wait on a job; it lapses 4 hours after the last renewal and is then taken over.
  A renewal that exits 4 means the lease was lost: stop at once and write nothing more.
  Release it with `hops factory system lease release {slug}` when you finish, stop or fail.
- Read `system.yaml` first. Resume from it: a phase whose `status` is `met` is done; start at the first one that is not.
- **Writing `system.yaml`**: never write it in place. Write the whole new file to `.system.yaml.edit-<n>` in this directory, then run `hops factory system write-doc {slug} --expected-sha256 <sha256sum of the system.yaml you read> --from .system.yaml.edit-<n>`.
  Exit 3 means it changed since you read it (the Hopsworks UI or a change request wrote it): read it again and redo your edit. Exit 2 means the new file is not valid YAML.
- The phases, in order: {phases}.
  When you start a phase, write `status: running` and `started` (UTC, ISO 8601) in its block; when it is done, `status: met` and `finished`.
  A phase you cannot finish gets `status: unmet` and a `reason`; stop and say why.
- Record what you create (jobs, feature groups, feature views, models, deployments, apps, dashboards) under `outputs` in `system.yaml`, each with its name and version, so `hops factory system delete <system> --assets` can delete it later.
  Name each after `{slug}`, or put `{slug}` in its description: the delete keeps anything else as not this system's.
- Keep the code in this directory, in a git repository, and push after each phase. `requirements.repo` says which: `{create: true}` is the private GitHub repository `hops-{slug}`, which you create with `gh repo create` when `gh auth status` succeeds; `{create: false, url}` is the user's existing repository at `url`, on GitHub, GitLab, Bitbucket or another host, which you push to with git alone. Record it as `system.repo: {url, provider}` (`provider` github, gitlab, bitbucket or git). The hops-reqs skill's `references/repo.md` covers access and other git hosts.
- Use the `hops` CLI for Hopsworks; never print or log secrets.
- **Change requests.** The Factory and `hops factory run <factory> <slug> --change <id>` record a change to the system as a `changes` entry with `status: pending`, holding its `label`, `answers` and `instructions`. Before anything else, carry out each pending one, oldest first: follow its `instructions` with its `answers`, then set its `status: done` and `finished` (UTC), or `status: failed` with a `reason` you also report, and commit `[<slug>] <label>`. Delete a job or a feature group only with `hops factory system delete-assets <slug> --job <name> --table <name>:<version>`, which refuses what the system reads.
{skills}
## The factory's instructions

{instructions}

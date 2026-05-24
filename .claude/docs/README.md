# Agent Docs

Detailed reference for AI coding agents.

## Architecture

- @docs/architecture/README.md — package map, dependency relationships, key files
- @docs/architecture/packages.md — each package in detail: purpose, structure, patterns
- @docs/architecture/domain.md — domain concepts and how they relate
- @docs/architecture/dependencies.md — dependency groups, version policy, how to add deps

## Development

- @docs/development/README.md — quick commands and workflow summary
- @docs/development/setup.md — environment setup, pre-commit, IDE config
- @docs/development/testing.md — test structure, fixtures, what and how to test
- @docs/development/linting.md — Ruff rules, docsig, pre-commit, common failures
- @docs/development/ci.md — CI jobs, what fails them, how to fix

## Conventions

- @docs/conventions/README.md — coding conventions summary
- @docs/conventions/docstrings.md — Google-style docstring guide with examples
- @docs/conventions/code-style.md — naming, imports, type hints, error handling, logging
- @docs/conventions/public-api.md — `@public`, `@deprecated`, linking in docstrings

## Caveats

- @docs/caveats/README.md — known gotchas and workarounds; add new ones here

## Wizard briefs

Orchestration briefs the in-app Wizard pastes into Claude Code in the Terminal. Each is a short conversational program: a few inputs from the user, then CLI-first execution.

- @docs/wizard/time-series.md — end-to-end ML system on Hopsworks: raw FGs to deployed Streamlit app
- @docs/wizard/research.md — autonomous research loop on a feature view, logged to a FG and the model registry
- @docs/wizard/friction-log.md — issues hit running the time-series brief end-to-end (ETH block-count regressor), with fix proposals

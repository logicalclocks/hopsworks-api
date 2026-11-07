---
name: hops-semantic-views
description: Use when creating or reshaping a semantic view on Hopsworks, the semantic project that agents and Superset 7.0 users read to answer questions with metrics. Scope it to one business domain and the questions it answers, scaffold it from only the feature groups those need, keep it under 100k tokens so it does not swamp an agent's context window, measure it, and split one that grew too large. Auto-invoke for a semantic view, a metric view, a semantic project meant for agents, or metric and dimension lists that come back cut. Input the questions and the feature groups that hold the data; output a compiled semantic project sized for agents.
---

# Semantic Views on Hopsworks

A semantic view is one semantic project: the semantic models, metrics and saved queries that one set of questions needs.
Hopsworks serves it to agents through the `hops semantic` CLI and the MCP tools, and to Superset 7.0 as one semantic view per semantic project plus one per saved query, named `<semantic project>.<saved query>`.
The YAML format, compiling and querying are in [hops-semantic](../hops-semantic/SKILL.md); this skill covers what goes in a view and how large it may get.

## Contract
- **Input:** the questions the view answers, and the feature groups that hold the data.
- **Output:** a compiled semantic project under 100k tokens, with saved queries for the common questions.
- **Pre-condition:** the feature groups exist with primary keys and event time ([hops-fg](../../ml/hops-fg/SKILL.md)).

## Smoke-test (cheap pre/post-flight)
```bash
hops semantic list                       # existing views; reuse one whose domain fits before creating another
hops semantic show <name>                # root path, and the build that answers queries
hops semantic metrics <name>             # what an agent sees first
```

## Budget: under 100k tokens per view

Keep each view under 100k tokens, counting what an agent reads to change it and what it reads to query it:
- the semantic YAML under the root path (`semantic_models:`, `metrics:`, `saved_queries:`);
- the discovery lists: metrics, dimensions, entities and saved queries.

An agent loads these into its context window before it writes a query.
A view larger than that leaves too little room for the question, the query results and the agent's reasoning, and answers degrade well before the window is full.
The MCP tools also cut each list at 50,000 characters and add a line saying so: an agent never sees the metrics or dimensions past the cut.

Estimate tokens as characters / 3, since YAML and CSV full of snake_case identifiers run at about 3 characters per token: 100k tokens is about 300,000 characters.

## Measure

```bash
hops semantic show sales      # root path /Projects/<project>/<dir>; a terminal or job mounts it at /hopsfs/<dir>
D=/hopsfs/Resources/semantic/sales
grep -rlE --include='*.yml' --include='*.yaml' '^(semantic_models|metrics|saved_queries):' "$D" \
  | xargs -r cat | wc -c                                             # the semantic YAML
for list in metrics dimensions entities saved-queries; do
  printf '%-14s' "$list"; hops semantic "$list" sales --json | wc -c   # keep each list under 50,000
done
```

Add the YAML and the four lists, and divide by 3.
The `--json` lists are larger than the CSV the MCP tools return, so the estimate errs on the safe side.
Measure again after every change that adds metrics, models or dimensions.

## Scope a view

- **One view per business domain or team** (sales, finance, support), named after it, rather than one per warehouse or data source.
  The Factory's gold layers already work this way: each data mart registers as its own semantic project ([hops-analytics](../hops-analytics/SKILL.md)).
- **Start from the questions.** Write down the 10 to 20 questions the view answers, then add only the metrics and dimensions they need.
- **Only the feature groups those metrics read**, plus the ones their dimensions join through: `hops semantic init sales --from-fg orders --from-fg customers` with nothing else.
- **Dimensions people group or filter by.** Every categorical dimension of a joined model is listed for every metric that reaches it, so the dimensions list grows with each join.
  Drop IDs, free text, and columns nobody slices by from the scaffold's `dimensions:`.
- **Entities only for the joins the view uses.**
- **One metric per measure and filter that matters.** `revenue` grouped by `customer__region` answers what `revenue_eu` and `revenue_us` would, at a fraction of the listing.
- **Short labels, one-sentence descriptions.** A description that restates the name costs tokens and tells the agent nothing; one that says the unit, the filter and when to use the metric earns its place.
- **Saved queries for the most-asked questions.** An agent runs one by name with `query_saved_query`, without discovering group-bys first, and Superset gets a view of it.

## Split a view that grew too large

- Split by domain into semantic projects with their own root paths.
  Two semantic projects can model the same feature group, so a shared table such as customers goes into each view that joins it.
- Move rarely asked metrics into a view of their own rather than deleting them; `hops search <word> --type metric` still finds them across views.
- Compile and measure each view after the split.

## Steps (generic, non-binding)
1. Write down the questions and pick the domain.
2. `hops semantic init <domain> --from-fg <fg> ...` with only the feature groups the questions need.
3. Prune the scaffold: dimensions, entities; uncomment only the metrics needed and give them labels and one-sentence descriptions.
4. Add saved queries for the top questions ([hops-semantic](../hops-semantic/SKILL.md) has the format).
5. `hops semantic compile <name> --wait`, then `hops semantic test <name>`.
6. Measure; prune or split until the view is under 100k tokens and each list under 50,000 characters.

## Toolset
- **CLI:** `hops semantic init|compile|test|show|metrics|dimensions|entities|saved-queries`, `hops search --type metric|saved_query`
- **MCP:** `list_semantic_projects`, `list_metrics`, `get_dimensions`, `get_entities`, `list_saved_queries`, `query_saved_query`
- **SDK:** `project.get_semantic_layer_api().get_project(<name>)`

## Related skills
- [hops-semantic](../hops-semantic/SKILL.md): the YAML, metric types, compiling, querying and tags.
- [hops-superset](../../dashboards/hops-superset/SKILL.md): dashboards over the view in Superset 7.0.
- [hops-analytics](../hops-analytics/SKILL.md): gold data marts, one semantic project each.

---
name: hops-design
description: "The Hopsworks look and the layout rules for every visual an agent builds on Hopsworks: apps (Streamlit, custom HTML/JS), Superset dashboards, charts, tables and record lists. Force into context when designing or styling an app or dashboard, choosing colors, fonts or spacing, laying out a UI that sits in a narrow column beside a chat, or showing records, numbers, statuses or charts. Reference companion to hops-app and hops-superset."
---

# Hopsworks design

## Concept
An app or dashboard built on Hopsworks opens inside the Hopsworks UI, often in a
narrow panel beside a chat. It should read as part of the platform: light, flat,
quiet, with the green accent where it means something. The page shows the data
and the actions on it; it does not explain itself.

## Key facts / rules

### Tokens (hopsworks-front)

| Token | Value | Use |
|---|---|---|
| background | `#ffffff` | the page and every surface |
| muted | `#f7f7f7` | a tinted region, an input, a hovered row |
| border | `#e5e5e5` | hairlines: dividers, table rules, inputs |
| text | `#252525` | body and headings |
| muted text | `#737373` | labels, metadata, units, timestamps |
| accent | `#1eb182` | the one primary action, the selection, a positive state |
| accent soft | `#e6f2ee` | the background of a selected or positive item |
| danger | `#e7000b` | errors and destructive actions only |
| radius | `10px` (controls `8px`) | a child's radius is at most its parent's |
| font | `Inter, system-ui, sans-serif` | everything |
| mono | `"IBM Plex Mono", ui-monospace, monospace` | code, ids, paths, SQL |

- Light theme only: the app renders inside the light Hopsworks UI. No dark mode, no
  `prefers-color-scheme` switch.
- Accent sparingly: one primary button per view, the current selection, a positive
  status. Everything else is neutral grey.
- No gradients, no banners, no emoji in titles or labels, no logo badge.

### Surfaces and nesting
- No shadow unless it means "floats above the page": a popover, a menu, a sticky
  toolbar over scrolling content. Cards, panels and buttons are flat.
- A white page. Sections are grouped by spacing and at most a hairline between
  them, never a bordered card per section; a filter bar sits above what it
  filters, not in a box of its own.
- Use fewer borders. Group with spacing first, then a muted tint, then a hairline
  divider; a bordered box is the last option.
- One level of box by default. A second level is justified only for:
  - foreign content: an iframe, a code block, a rendered file, a chart canvas;
  - an alert or callout that must stand apart from the flow;
  - a group that spacing cannot show, such as a form beside unrelated content;
  - items that are the content itself, such as nodes in a graph.
- Never a box around a list or table that already has row rules, and never a list or
  table squeezed into a small box.

### Vertical rhythm (narrow column beside a chat)
- One column that stacks. Every grid column is `minmax(0, 1fr)`; a table scrolls
  inside its own wrapper (`overflow-x: auto`), never the page.
- Check the layout at about 600px and 900px wide.
- One spacing scale (4, 8, 12, 16, 24, 32px). Related things sit closer together
  than unrelated ones; a section gap is at least twice the gap inside a section.
- Nothing shifts when data loads or reloads: keep the old content dimmed until the
  new arrives, reserve the scrollbar gutter, size placeholders like the content.
- No empty gaps: space is reserved only for content that will load into it, at
  that content's size.

### Hierarchy and copy
- At most three type sizes per view: the title, body, small (labels and metadata).
  Emphasise by de-emphasising the rest (muted text), not by adding sizes or colors.
- Show, do not tell: no intro paragraph, no "This dashboard shows...", no
  marketing copy. A heading names the thing; the numbers and charts speak.
- Labels are short nouns. Units sit with the number (`42 ms`, `1.2k rows`).

### Records: lists and tables
- Records of one kind go in a list or a table, never a grid of cards. Cards are for
  mixed content to browse.
- A list row: one label line, one muted metadata line. Fixed positions in every row.
- A table: text left-aligned, numbers right-aligned with
  `font-variant-numeric: tabular-nums`, never centered. Header in muted text.
- Status is a dot plus a word (`● materialized`), never color alone and never a
  pill per row.
- Names sharing a prefix wrap to two lines rather than truncate; when a value must be
  cut, keep its full text in a `title`.
- One or two row actions inline; more go in a menu. Every row leads somewhere.
- Say when only part is shown: `20 of 1,204`, with a link to the rest.

### Numbers and stats
- Key figures as one row of stats separated by hairlines, not one box each: muted
  label, value, optional muted delta or context.
- A value never overflows its cell: every column `minmax(0, 1fr)` with
  `min-width: 0`, the value sized to the cell (`font-size: clamp(...)` in container
  units), on one line, cut with an ellipsis and its full text in `title` past the
  smallest size. Check the longest figure the data can produce (`$1,803,299.12`).
- Tabular numerals wherever numbers line up or update.

### Charts
- A charting library, never hand-drawn SVG: Chart.js in a custom app (the hops-app
  skeleton's `static/vendor/` and `static/chart-theme.js`), Superset's charts in a
  dashboard, matplotlib or seaborn in Python.
- The accent for the primary series; greys (`#a0a0a0`, `#cbcbcb`) for context and
  comparison; danger only for a threshold breach.
- An area is its line plus a fill of the same color at low opacity (about 12%);
  a closed shape with no fill renders black.
- No 3D, no shadows, no background fills, light or no gridlines, no legend for a
  single series. Label axes with units; title the chart with the question it answers.
- Tick labels 12px in muted text, never rotated, skipped when they crowd, and never
  clipped at the edges: pad the plot area. The chart takes its container's width and
  a fixed height.
- Same scale on charts meant to be compared.

## Docs
- Refactoring UI, "use fewer borders": https://www.refactoringui.com/
- NN/g, common region and cards: https://www.nngroup.com/articles/common-region/ ,
  https://www.nngroup.com/articles/cards-component/
- Vercel Web Interface Guidelines: https://vercel.com/design/guidelines

## Related skills
- [hops-app](../hops-app/SKILL.md): Streamlit theme and the custom app skeleton
  (`references/app_skeleton/static/app.css` and `chart-theme.js` carry these tokens).
- [hops-superset](../hops-superset/SKILL.md): dashboards and charts in Superset.
- [hops-reqs](../../ml/hops-reqs/SKILL.md): `charts.py` draws a model's registry charts
  in matplotlib with these tokens; [hops-train](../../ml/hops-train/SKILL.md),
  [hops-eda](../../ml/hops-eda/SKILL.md), [hops-monitoring](../../ml/hops-monitoring/SKILL.md)
  and [hops-analytics](../../data/hops-analytics/SKILL.md) chart the same way.

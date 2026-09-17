Read the hopsworks-api source code, found in ${HOME}/hopsworks-api, for how to connect to data sources, save data/features/models, deploy models/apps/agent, use feature groups, use feature views, create transformations, and build offline/online ML systems as well as agents, streamlit apps, and superset dashboards.

Skills are organized into bucket folders under skills/ at the repo root. The
terminal images clone this repo to /opt/hopsworks-api and symlink each skill
into every agent's skills directory, so they are served from the image rather
than copied into a user's project home:

ml/ — develop and operate ML systems with FTI pipeline architecture (feature groups, feature views, transformations, training, inference, monitoring)
agents/ — develop and operate agent tasks and agent deployments
dashboards/ — Streamlit / custom apps and Superset dashboards
data/ — data discovery, data sources, Trino SQL, dbt, Spark, table maintenance
platform/ — cross-cutting platform knowledge and compute (jobs, environments, UI navigation, collaboration, kubectl debugging), not tied to one FTI stage
Every skill under skills/ must have a reference in the top-level README.md.

Each skill entry in the top-level README.md must link the skill name to its SKILL.md.

Each bucket folder has a README.md that lists every skill in the bucket with a one-line description, with the skill name linked to its SKILL.md.

A SKILL.md carries what every task using the skill needs; deep dives, copy-paste variants and long tables go in the skill's references/ folder and are linked from SKILL.md, so an agent that auto-loads the skill does not pay for detail it may not need.

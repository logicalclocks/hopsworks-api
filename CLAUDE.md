Read the hopsworks-api source code, found in ${HOME}/hopsworks-api, for how to connect to data sources, save data/features/models, deploy models/apps/agent, use feature groups, use feature views, create transformations, and build offline/online ML systems as well as agents, streamlit apps, and superset dashboards.

Skills are organized into bucket folders under skills/:

ml/ — develop and operate ML systems with FTI pipeline architecture
agents/ — develop and operate scheduled agents and agent deployments
apps/ —  design and operate Streamlit apps
dashboards/ —  design Superset dashboards
deprecated/ — no longer used
Every skill in ml/, agents/, apps/, dashboards/ must have a reference in the top-level README.md and an entry in .claude-plugin/plugin.json. Skills in deprecated/ must not appear in either.

Each skill entry in the top-level README.md must link the skill name to its SKILL.md.

Each bucket folder has a README.md that lists every skill in the bucket with a one-line description, with the skill name linked to its SKILL.md.

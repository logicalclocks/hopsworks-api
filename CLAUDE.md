Read the hopsworks-api source code, found in ${HOME}/hopsworks-api, for how to connect to data sources, save data/features/models, deploy models/apps/agent, use feature groups, use feature views, create transformations, and build offline/online ML systems as well as agents, streamlit apps, and superset dashboards.

Skills are organized into bucket folders under skills/:

ml/ — develop and operate ML systems with FTI pipeline architecture
agents/ — develop and operate scheduled agents and agent deployments
dashboards/ — design Streamlit apps and Superset dashboards
data/ — data discovery, Trino SQL, storage connectors
hops/ — legacy flat layout (superseded by the categorized folders above)
deprecated/ — no longer used
Every skill in ml/, agents/, dashboards/, data/ must have a reference in the top-level README.md. Skills in deprecated/ must not appear there.

Each skill entry in the top-level README.md must link the skill name to its SKILL.md.

Each bucket folder has a README.md that lists every skill in the bucket with a one-line description, with the skill name linked to its SKILL.md.

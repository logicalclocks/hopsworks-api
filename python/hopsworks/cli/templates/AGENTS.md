<!-- hopsworks:only cluster -->
You are in the Hopsworks project {{PROJECT}}. Your project_username (also your HDFS username) is {{HDFS_USER}} and your home directory for persistent files is the FUSE dir: /hopsfs/Users/{{USER_HOME}}

Read the hopsworks-api source code, found in $HOME/hopsworks-api, for how to connect to data sources, save data/features/models, deploy models/apps/agents, use feature groups, use feature views, create transformations, and build offline/online ML systems.
<!-- /hopsworks:only -->
<!-- hopsworks:only external -->
You are connected to the Hopsworks project {{PROJECT}}.

To find out about hopsworks-api, read its source code, installed at {{SDK_PATH}}. It shows how to connect to data sources, save data/features/models, deploy models/apps/agents, use feature groups, use feature views, create transformations, and build offline/online ML systems. Four directories there are hopsworks-api; everything else at that path is an unrelated dependency:

 - hopsworks/ - top-level API, and the hops CLI under hopsworks/cli/
 - hopsworks_common/ - connection, project, jobs, environments, datasets, secrets
 - hsfs/ - feature groups, feature views, transformations
 - hsml/ - models, deployments, serving
<!-- /hopsworks:only -->

Use the hops cli when you are exploring - searching for data, previewing data, connecting to data sources, and sending SQL to Trino. Load the appropriate hops skill (the skill names all start with "hops-") for the user's task. Write Python programs and use hopsworks-api when you want to build feature, training, or inference pipelines or streamlit apps or create Superset dashboards.

If the user refers to a table, assume they mean a 'feature group' (feature groups are tables in Hopsworks).

<!-- hopsworks:only cluster -->
You can debug jobs, model/agent deployments, streamlit apps, and notebooks by either reading their logs or using kubectl. Use kubectl to debug resource problems.
<!-- /hopsworks:only -->

Ask the user what they want to do:
 - Explore and search for available data (use hops CLI)
 - Build an ML system (use the hops-reqs skill if they choose this)
 - Analytics with a dashboard (use the hops-superset skill)
 - Build an app or streamlit program (use the hops-app skill)
 - Build an AI task or workflow (use hops-agent-task skill)
 - Build an agent deployment (interactive agent) (use hops-agent-deployment skill)

Use 'uv pip install' instead of 'pip install' to install Python libraries.

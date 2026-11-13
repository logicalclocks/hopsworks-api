### Git-backed agents

When the source lives in Git, give the repository instead of a local path. The repository is cloned on each start, so a restart or redeploy picks up new commits. Providers: `GitHub`, `GitLab`, `BitBucket`; a private repository needs the user's git provider configured in Hopsworks.

```python
agent = agents.deploy_agent(
    entry="agent.py",                    # repo-relative
    name="my_agent",
    git_url="https://github.com/acme/my-agent-repo.git",
    git_provider="GitHub",
    git_branch="main",
    git_auto_redeploy=True,              # roll to branch HEAD on every new commit
    environment="my_agent",
    framework="langgraph",               # not read from a git source: always pass it
    chat_config="hopsworks",
)
```

`hops agent info` shows the git source, the resolved branch and the commit the deployment is running.

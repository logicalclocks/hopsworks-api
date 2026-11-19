# Git-backed apps

Part of **hops-app**: read when the app's source is a GitHub, GitLab or BitBucket repository.

Leave `app_path` unset and provide the repository fields. Git-backed apps are
cloned again on each start, so a restart or redeploy picks up new commits;
`git_auto_redeploy=True` rolls to the branch HEAD on every new commit. Supported
providers: `GitHub`, `GitLab`, `BitBucket`.

- Streamlit, Flask, Gradio, Node.js: the kind and `entrypoint_script` (the app file) relative to the repository root; `app_port` for all but Streamlit.
- Custom: `app_kind="CUSTOM"`, an `entrypoint_command`, and `app_port`.

```python
streamlit_app = apps.create_app(
    name="streamlitfromgithub",
    app_kind="STREAMLIT",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    git_branch="main",
    git_auto_redeploy=True,
    entrypoint_script="streamlitapp.py",
    environment="python-app-pipeline",
)

node_app = apps.create_app(
    name="greeterfromgithub",
    app_kind="NODEJS",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_script="npm-packages/greeterapp.mjs",
    app_port=8080,
    environment="my_cloned_env",          # with express installed via install_npm
)

fastapi_app = apps.create_app(
    name="fastapifromgithub",
    app_kind="CUSTOM",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_command=(
        'bash -lc "python -m uv pip install --no-cache fastapi uvicorn && '
        'exec python -m uvicorn fastapiapp:app --host 0.0.0.0 --port \\"$APP_PORT\\""'
    ),
    app_port=8080,
    environment="python-app-pipeline",
)
fastapi_app.run()
print(fastapi_app.app_url)
```

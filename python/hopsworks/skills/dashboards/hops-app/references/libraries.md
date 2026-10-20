# Custom libraries in an app

Part of **hops-app**: read when the app needs a library its environment lacks, or loads a pickled model.

If the app needs libraries not in `python-app-pipeline`, clone that base env and
install the app's `app-requirements.txt` into the **clone** (never the base). It
takes a few minutes; warn the user, and prompt for a requirements file if none
exists. Full workflow, including the project-path rule for the requirements
file: [hops-environments](../../../platform/hops-environments/SKILL.md).

```python
env_api = project.get_environment_api()
# get_environment returns None for a missing environment, it does NOT raise:
# guard with `is None`, a try/except never fires.
cloned_env = env_api.get_environment("my_cloned_env")
if cloned_env is None:
    cloned_env = env_api.create_environment(
        "my_cloned_env", base_environment_name="python-app-pipeline",
    )
cloned_env.install_requirements("Users/<username>/app-requirements.txt", await_installation=True)
```

Then pass `environment="my_cloned_env"` to `create_app(...)`, not the base.

If the app loads a pickled model (e.g. scikit-learn via `joblib`), the base
`python-app-pipeline` may ship a different scikit-learn than the one that pickled
it, and the load fails. The base env exposes no library list (no `get_libraries`),
so pin the model's exact training versions (e.g. `scikit-learn==1.8.0` plus
matching `numpy`/`scipy`/`joblib`) in the requirements file.

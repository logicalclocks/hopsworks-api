---
name: hops-notebook
description: Use when writing or running a Jupyter notebook for the user, from outside JupyterLab. Input a notebook path and cells; output an .ipynb with its outputs, run on the user's own Jupyter kernel, that opens in their JupyterLab.
---

# Notebooks on the user's Jupyter kernel

`hops notebook` writes cells into an `.ipynb` in the project and runs them on the user's own
Jupyter server, the one JupyterLab in Hopsworks uses. The outputs are saved in the file, so the
notebook opens in JupyterLab as the run left it.

## Contract
- **Input:** the notebook's path in the project, in the user's Jupyter folder
  (`Users/<username>/<name>.ipynb`; `hops notebook start` prints the folder), and one cell at a time.
- **Output:** the `.ipynb` with every cell and its outputs; each `run` prints the cell's outputs.
- **Pre-condition:** none; `run` starts the Jupyter server when it is not running.

## Commands

```bash
hops notebook start                     # prints its Python environment and the folder, Users/<username>
hops notebook run Users/alice/churn.ipynb --code - <<'EOF'
import hopsworks
fs = hopsworks.login().get_feature_store()
EOF
hops notebook run Users/alice/churn.ipynb --markdown --code "## Churn by plan"
hops notebook run Users/alice/churn.ipynb --code "df = fs.get_feature_group('customers', version=1).read()"
hops notebook run Users/alice/churn.ipynb --cell 3 --code "..."   # replace cell 3 and run it again
hops notebook info Users/alice/churn.ipynb          # environment, kernel state, cells, run numbers, outputs
hops notebook stop                      # stops the server and its kernels
hops notebook start --env <name>        # a not running server, in that environment (`hops env list`)
```

- One kernel per notebook: variables carry over from cell to cell, and JupyterLab joins the same
  kernel when the user opens the notebook.
- A cell that raises exits 1 with the error after saving it; fix it with `--cell N`.
- A cell still running after `--timeout` (600 s) is interrupted. Long work belongs in a job
  (`hops-job`), with the notebook reading what the job wrote.
- Inside the kernel, `hopsworks.login()` takes no arguments: the server runs in the project.
- The kernel's packages are those of the server's Python environment, which `start` and `info`
  print. A missing package goes into that environment, a clone of a base one (`hops-environments`
  skill: `hops env clone`, `hops env install`), then `hops notebook start --env <clone>` after a
  `stop`. Never `pip install` in a cell: it is gone when the server restarts. `--env` refuses to
  replace a server running in another environment: stopping it ends the user's kernels, so ask first.
- Charts: `matplotlib` or `plotly`, one chart per cell; the figure is saved as the cell's output.

## Rules
- Run each cell before writing the next, so a failure is fixed where it happens.
- Never change a cell you did not write unless the user asks: the user may be editing the notebook
  in JupyterLab, and a save replaces the file.
- Done when `hops notebook info` shows a `RUN` number on every code cell and no cell raised.

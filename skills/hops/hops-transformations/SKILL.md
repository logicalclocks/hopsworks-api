---
name: hops-transformations
description: Use when writing Python code for Hopsworks transformation functions — built-in transforms, custom @udf transforms, model-dependent (statistics-based) transforms, on-demand (request-time) transforms, and the transformation store. Auto-invoke when the user applies scaling/encoding/imputation to features, writes a @udf, creates on-demand features from request parameters, registers a transformation function, or asks how transformation renaming/execution modes work. Input: a feature and a transform → Output: a transformation function attached to a feature view (model-dependent) or feature group (on-demand), or saved in the transformation store.
---

# Hopsworks Transformations

The T in FTI. A transformation function maps feature columns to transformed
columns, applied consistently at training and inference. Two kinds:

- **Model-dependent** — learn statistics from training data (scalers, encoders,
  imputers). Attached to a **feature view**; applied during training and serving.
- **On-demand** — computed at request time from request parameters, no statistics.
  Attached at the **feature group** level; auto-included by any FV selecting them.

## Contract
- **Input:** a feature (or features) and a transform (built-in or `@udf`).
- **Output:** a transformation function bound to an FV (model-dependent) or FG
  (on-demand), and optionally saved in the transformation store for reuse.
- **Pre-condition:** the feature exists; for statistics-based transforms the FV
  has materialized training data so statistics are available.

## Smoke-test (cheap pre/post-flight)
```bash
hops transformation list                                   # registered functions + output types
hops transformation create --code "@udf(float)
def x2(c): return c * 2" --version 1                        # register from inline source
hops transformation create --file my_udf.py --version 1     # or from a .py file
```

## The rename footgun (read first)
A transformation **renames its output column** to `<fn>_<col>_`
(`standard_scaler("amount")` → `standard_scaler_amount_`). The original column is
gone afterward, so training and serving must reference the **new** name. Renaming
the udf renames its outputs too. Use `.alias("custom_name")` to control the output
name.

## Built-in transformations (model-dependent)
Import from `hsfs.builtin_transformations`. All learn statistics from training data.
```python
from hsfs.builtin_transformations import standard_scaler, label_encoder, impute_mean

fv = fs.create_feature_view(
    name="my_fv", version=1, query=query, labels=["target"],
    transformation_functions=[
        impute_mean("age"), standard_scaler("age"), label_encoder("country"),
    ],
)
```
| Group | Functions |
|---|---|
| Scaling | `min_max_scaler`, `standard_scaler`, `robust_scaler` |
| Distribution | `log_transform`, `quantile_transformer`, `rank_normalizer` |
| Outliers | `winsorize` — ctx `{"p_low":5,"p_high":95}` |
| Binning | `equal_width_binner` ctx `{"n_bins":20}`, `equal_frequency_binner`, `quantile_binner` |
| Encoding | `label_encoder` (unseen→-1), `one_hot_encoder` (unseen→all False), `top_k_categorical_binner` ctx `{"top_n":20,"other_label":"Rare"}` |
| Imputation | `impute_mean`, `impute_median`, `impute_constant` ctx `{"value":-1.0}`, `impute_mode`, `impute_category` ctx `{"value":"Unknown"}` |

Set context on a built-in that accepts it:
```python
w = winsorize("income"); w.transformation_context = {"p_low": 5, "p_high": 95}
fv = fs.create_feature_view(name="my_fv", query=query, transformation_functions=[w])
```

## Custom transformations — the `@udf` decorator
**Import from `hopsworks`, not `hsfs`** (`from hsfs import udf` raises ImportError).
```python
from hopsworks import udf

@udf(float, drop=["amount"])
def log_amount(amount):
    import numpy as np
    return np.log1p(np.maximum(amount, 0))
```
| Param | Type | Meaning |
|---|---|---|
| `return_type` | `type` or `list[type]` | output type(s): `float`, `int`, `str`, `bool`, `datetime`, `date` |
| `drop` | `str` or `list[str]` | input features to drop after applying |
| `mode` | `"default"` / `"python"` / `"pandas"` | execution mode (see below) |

### Execution mode — the online HTTP-500 footgun
| Mode | Offline (batch) | Online (single) |
|---|---|---|
| `"default"` | `pd.Series` → `pd.Series` | scalar → scalar |
| `"pandas"` | always `pd.Series` → `pd.Series` | always `pd.Series` |
| `"python"` | scalar → scalar | scalar → scalar |

A **default-mode** udf runs on a `pd.Series` offline (training, `td compute`) and on
a **scalar** online (`get_feature_vector`) — the *same body*. Series-only methods
(`.clip`, `.fillna`, `.str`, `.between`, `.where`, `.dt`) raise on the scalar and
surface as an **HTTP 500 on the first online predict**, invisible until then. Use
numpy ufuncs / plain arithmetic that accept both, or set `mode="pandas"`. Smoke-test
both shapes before wiring the FV:
```python
import numpy as np, pandas as pd
logic = lambda x: np.log1p(np.maximum(x, 0))
assert logic(5.0) == logic(pd.Series([5.0])).iloc[0]   # scalar == Series path
```

### Statistics, context, multiple outputs, alias
```python
from hsfs.transformation_statistics import TransformationStatistics
stats = TransformationStatistics("price")

@udf(float, drop=["price"])
def z_score(price, statistics=stats):
    return (price - statistics.price.mean) / statistics.price.stddev
# stats props: mean, stddev, min, max, percentiles, unique_values, histogram,
#              count, completeness, distinctness, entropy

@udf(float)                                   # request-time context
def apply_discount(price, context): return price * (1 - context["discount_rate"])
tf = apply_discount("price"); tf.transformation_context = {"discount_rate": 0.1}

@udf([float, float], drop=["timestamp"])      # multiple outputs
def time_features(timestamp):
    import pandas as pd
    s = isinstance(timestamp, pd.Series)
    return (timestamp.dt.hour if s else timestamp.hour,
            timestamp.dt.dayofweek if s else timestamp.dayofweek)

tf = log_amount("price"); tf.alias("log_price")   # custom output column name
```

> **A udf is frozen into the feature view at `fv create`** — bound to the FV
> version, not resolved live from the registry. Fixing a udf means a new FV version
> → recompute training data → retrain → recreate the deployment. The most expensive
> thing to get wrong; smoke-test the scalar+Series logic before `fv create`.

## On-demand transformations (request-time)
Computed at inference from request parameters; **cannot** use training statistics.
Attached at the **feature group** level (`transformation_functions=` on
`get_or_create_feature_group`), auto-included by any FV that selects them.
```python
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    request_parameters={"current_location": "NYC"},
)
```

## Transformation store (register, reuse)
A `@udf` must live in a **real `.py` file** — the SDK extracts its source via
`inspect.getsource`, so functions defined in a REPL/stdin raise
`OSError: could not get source code`. Registration is lazy; call `.save()`.
```python
from hopsworks import udf

@udf(float, drop=["x"])
def double(x): return x * 2

tf = fs.create_transformation_function(transformation_function=double, version=1)
tf.save()                                          # persists in the store
fs.get_transformation_functions()                  # list (built-ins included)
fs.get_transformation_function(name="double", version=1)
```

## Toolset
- **CLI:** `hops transformation list`, `hops transformation create --file|--code [--version]`.
- **SDK:** `from hopsworks import udf`; `hsfs.builtin_transformations`; `fs.create_transformation_function()` / `get_transformation_function(s)()`; `transformation_functions=` on `create_feature_view` (model-dependent) and `get_or_create_feature_group` (on-demand).
- **Source:** `python/hsfs/hopsworks_udf.py`, `builtin_transformations.py`, `transformation_statistics.py`, `python/hopsworks/cli/commands/transformation.py`.

## Next steps
- [hops-fv](../hops-fv/SKILL.md) — attach model-dependent transforms; the rename propagates to training/serving names.
- [hops-fg](../hops-fg/SKILL.md) — attach on-demand transforms at the feature group.
- [hops-online-inference](../hops-online-inference/SKILL.md) — request_parameters at serving; the scalar-path footgun bites here.
- [hops-train](../hops-train/SKILL.md) — training data reflects the transformed (renamed) columns.

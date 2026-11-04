#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""The check of a local predictor script's `Predict.__init__`, apart from hsml.predictor so the CLI can run it before importing the rest of hsml."""

import ast
import os

from hopsworks_common.constants import MODEL_SERVING


def check_predict_init(script_file: str) -> None:
    """Refuse a local script whose `Predict.__init__` the serving runtime cannot call.

    The runtime builds `Predict(**kwargs)` from `MODEL_SERVING.PREDICT_INIT_ARGS`
    only, so any other name, `*args`, `**kwargs` or a positional-only parameter
    fails after the deployment starts.
    """
    if not os.path.isfile(script_file):
        return
    with open(script_file, "rb") as f:
        source = f.read()
    try:
        tree = ast.parse(source, filename=script_file)
    except (SyntaxError, ValueError):
        return
    init = None
    for stmt in tree.body:
        if isinstance(stmt, ast.ClassDef) and stmt.name == "Predict":
            for item in stmt.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    init = item
    if init is None:
        return
    a = init.args
    positional = a.posonlyargs + a.args
    instance = positional[:1]  # self, bound by the call
    refused = [f"{p.arg} (positional-only)" for p in a.posonlyargs if p not in instance]
    refused += [
        p.arg
        for p in a.args + a.kwonlyargs
        if p not in instance and p.arg not in MODEL_SERVING.PREDICT_INIT_ARGS
    ]
    if a.vararg is not None:
        refused.append(f"*{a.vararg.arg}")
    if a.kwarg is not None:
        refused.append(f"**{a.kwarg.arg}")
    if refused:
        raise ValueError(
            f"{script_file}:{init.lineno}: Predict.__init__ takes {', '.join(refused)}, "
            "which the serving runtime cannot pass. Besides self, it accepts only "
            f"{', '.join(MODEL_SERVING.PREDICT_INIT_ARGS)}, passed by keyword."
        )
